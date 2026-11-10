package reconciler

import java.nio.file.{Files, Path}
import java.time.LocalDate
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.FileIO
import org.apache.pekko.util.ByteString

/** `POST /settlements/{date}/reconciliation`
  *
  * The request body is the acquirer's settlement file for that date; the ledger side is our own
  * export, read from `ledgerDir/ledger-{date}.csv`. The response streams the results as NDJSON, then a
  * final `{"end":true,"summary":…}` line. A response without that line was cut short (e.g. an unsorted
  * file stopped the run) and its results must be discarded.
  *
  * The upload is first written to a temporary file, streamed with constant memory, and only then
  * reconciled from disk while the response streams. Reconciling straight from the upload would tie
  * the request and the response together through backpressure: a client that sends the whole body
  * before reading the response (most HTTP/1.1 clients, e.g. Python's requests) would fill the network
  * buffers with results it isn't reading yet, and both sides would wait for each other until the
  * idle timeout. The temporary file is deleted when the response ends, however it ends.
  */
final class ReconciliationRoutes(ledgerDir: Path):

  val route: Route =
    path("settlements" / Segment / "reconciliation") { rawDate =>
      post {
        Try(LocalDate.parse(rawDate)).toOption match
          case None => complete(StatusCodes.BadRequest -> s"invalid date: $rawDate (expected YYYY-MM-DD)")
          case Some(date) =>
            val ledgerFile = ledgerDir.resolve(s"ledger-$date.csv")
            if !Files.isRegularFile(ledgerFile) then complete(StatusCodes.NotFound -> s"no ledger export for $date")
            else
              extractRequestEntity { entity =>
                extractMaterializer { materializer =>
                  given Materializer = materializer
                  val upload         = Files.createTempFile("settlement-", ".csv")
                  upload.toFile.deleteOnExit() // backstop, in case a response never runs
                  // The settlement body is streamed to disk, never held in memory, so the server's
                  // default request size limit (8 MB) would only cut it short: it's lifted here.
                  val stored = entity.withoutSizeLimit().dataBytes.runWith(FileIO.toPath(upload))
                  onComplete(stored) {
                    case Success(_) =>
                      val ndjson = Reconciler
                        .reconcile(FileIO.fromPath(ledgerFile), FileIO.fromPath(upload))
                        .map(output => ByteString(JsonLines.encode(output) + "\n"))
                        .watchTermination() { (notUsed, done) =>
                          done.onComplete(_ => Files.deleteIfExists(upload))(ExecutionContext.parasitic)
                          notUsed
                        }
                      complete(HttpResponse(entity = HttpEntity(ReconciliationRoutes.NdJson, ndjson)))
                    case Failure(error) =>
                      Files.deleteIfExists(upload)
                      complete(StatusCodes.BadRequest -> s"could not read the settlement upload: ${error.getMessage}")
                  }
                }
              }
      }
    }

object ReconciliationRoutes:

  val NdJson: ContentType.WithFixedCharset =
    MediaType.applicationWithFixedCharset("x-ndjson", HttpCharsets.`UTF-8`).toContentType
