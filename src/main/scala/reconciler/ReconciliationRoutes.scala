package reconciler

import java.nio.file.{Files, Path}
import java.time.LocalDate
import scala.util.Try
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.scaladsl.FileIO
import org.apache.pekko.util.ByteString

/** `POST /settlements/{date}/reconciliation`
  *
  * The request body is the acquirer's settlement file for that date; the ledger side is our own
  * export, read from `ledgerDir/ledger-{date}.csv`. The response streams the results as NDJSON while
  * both files are being read, then a final `{"end":true,"summary":…}` line. A response without that
  * line was cut short (e.g. an unsorted file stopped the run).
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
                // Settlement files are streamed, never held in memory, so the server's default request
                // size limit (8 MB) would only cut them short: it's lifted for this route.
                val settlement = entity.withoutSizeLimit().dataBytes
                val ndjson = Reconciler
                  .reconcile(FileIO.fromPath(ledgerFile), settlement)
                  .map(output => ByteString(JsonLines.encode(output) + "\n"))
                complete(HttpResponse(entity = HttpEntity(ReconciliationRoutes.NdJson, ndjson)))
              }
      }
    }

object ReconciliationRoutes:

  val NdJson: ContentType.WithFixedCharset =
    MediaType.applicationWithFixedCharset("x-ndjson", HttpCharsets.`UTF-8`).toContentType
