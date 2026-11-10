package reconciler

import java.net.Socket
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import scala.concurrent.{blocking, Future}
import scala.concurrent.duration.*
import io.circe.Json
import io.circe.parser.parse
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.scaladsl.{FileIO, Framing, Sink, Source}
import org.apache.pekko.util.ByteString

class ReconciliationRoutesSpec extends StreamSuite:

  override val munitTimeout: Duration = 2.minutes

  private val date = "2026-11-10"

  /** A ledger directory with the sample ledger export for `date`. */
  private def ledgerDir(): Path =
    val dir = Files.createTempDirectory("ledger")
    Files.copy(Path.of(s"sample-data/ledger-$date.csv"), dir.resolve(s"ledger-$date.csv"))
    dir

  private def post(route: Route, path: String, body: RequestEntity): Future[HttpResponse] =
    Route.toFunction(route).apply(HttpRequest(HttpMethods.POST, Uri(path), entity = body))

  /** The response's NDJSON lines, parsed. The client's own response size limit (8 MB) is lifted,
    * like the server's request limit, since a large reconciliation streams a large response.
    */
  private def lines(response: HttpResponse): Source[Json, ?] =
    response.entity
      .withoutSizeLimit()
      .dataBytes
      .via(Framing.delimiter(ByteString("\n"), 64 * 1024, allowTruncation = true))
      .map(line => parse(line.utf8String).fold(error => fail(s"invalid JSON line: $error"), identity))

  private def ndjson(response: HttpResponse): Future[Seq[Json]] = runToSeq(lines(response))

  private def summaryOf(last: Json): Json =
    last.hcursor.downField("summary").focus.getOrElse(fail(s"no summary in $last"))

  test("streams the results of the uploaded settlement file as NDJSON, ending with the summary") {
    val route      = ReconciliationRoutes(ledgerDir()).route
    val settlement = HttpEntity(ContentTypes.`text/csv(UTF-8)`, Files.readAllBytes(Path.of(s"sample-data/settlement-$date.csv")))
    for
      response <- post(route, s"/settlements/$date/reconciliation", settlement)
      lines    <- ndjson(response)
    yield
      assertEquals(response.status, StatusCodes.OK)
      assertEquals(response.entity.contentType.mediaType.toString, "application/x-ndjson")
      assert(response.entity.isChunked, "the results are streamed")
      assertEquals(lines.size, 10) // 9 results, then the summary
      assertEquals(lines.last.hcursor.get[Boolean]("end").toOption, Some(true))
      assertEquals(summaryOf(lines.last).hcursor.get[Long]("matched").toOption, Some(3L))
      assertEquals(summaryOf(lines.last).hcursor.get[BigDecimal]("matchedAmount").toOption, Some(BigDecimal("225.40")))
      val types = lines.init.flatMap(_.hcursor.get[String]("type").toOption)
      assertEquals(types.count(_ == "discrepancy"), 2)
      assertEquals(types.count(_ == "rejected"), 1)
  }

  test("an invalid date is a 400 and a date without a ledger export is a 404") {
    val route = ReconciliationRoutes(ledgerDir()).route
    for
      invalid <- post(route, "/settlements/10-11-2026/reconciliation", HttpEntity.Empty)
      missing <- post(route, "/settlements/2020-01-01/reconciliation", HttpEntity.Empty)
    yield
      assertEquals(invalid.status, StatusCodes.BadRequest)
      assertEquals(missing.status, StatusCodes.NotFound)
  }

  test("shards must be a number from 1 to 16") {
    val route = ReconciliationRoutes(ledgerDir()).route
    Future
      .traverse(Seq("0", "17", "two")) { shards =>
        post(route, s"/settlements/$date/reconciliation?shards=$shards", HttpEntity.Empty).map(response => shards -> response.status)
      }
      .map(statuses => statuses.foreach((shards, status) => assertEquals(status, StatusCodes.BadRequest, shards)))
  }

  test("over real HTTP, a reconciliation in 2 shards streams every result, then the expected summary") {
    val count = 200_000L
    val dir   = Files.createTempDirectory("sharded-ledger")
    for
      _        <- Generator.ledger(count).runWith(FileIO.toPath(dir.resolve(s"ledger-$date.csv")))
      binding  <- Http().newServerAt("127.0.0.1", 0).bind(Route.toFunction(ReconciliationRoutes(dir).route))
      uri       = s"http://127.0.0.1:${binding.localAddress.getPort}/settlements/$date/reconciliation?shards=2"
      upload    = HttpEntity(ContentTypes.`text/csv(UTF-8)`, Generator.settlement(count))
      response <- Http().singleRequest(HttpRequest(HttpMethods.POST, Uri(uri), entity = upload))
      // Only the line count and the last line are kept, not every result.
      (lineCount, last) <- lines(response).runFold((0L, Json.Null))((acc, line) => (acc._1 + 1, line))
      _                 <- binding.unbind()
    yield
      assertEquals(response.status, StatusCodes.OK)
      assertEquals(response.entity.contentType.mediaType.toString, "application/x-ndjson")
      val expected = Generator.expectedSummary(count)
      val summary  = summaryOf(last).hcursor
      assertEquals(summary.get[Long]("matched").toOption, Some(expected.matched))
      assertEquals(summary.get[Long]("discrepancies").toOption, Some(expected.discrepancies))
      assertEquals(summary.get[Long]("missingInSettlement").toOption, Some(expected.missingInSettlement))
      assertEquals(summary.get[Long]("missingInLedger").toOption, Some(expected.missingInLedger))
      assertEquals(summary.get[BigDecimal]("matchedAmount").toOption, Some(expected.matchedAmount.toBigDecimal))
      // One line per transaction from both shards, then the summary.
      val transactions = expected.matched + expected.discrepancies + expected.missingInSettlement + expected.missingInLedger
      assertEquals(lineCount, transactions + 1)
  }

  test("over real HTTP, a settlement file larger than the 8 MB request limit is reconciled in full") {
    val count = 500_000L // about 12 MB of settlement file, uploaded as a stream
    val dir   = Files.createTempDirectory("large-ledger")
    for
      _       <- Generator.ledger(count).runWith(FileIO.toPath(dir.resolve(s"ledger-$date.csv")))
      binding <- Http().newServerAt("127.0.0.1", 0).bind(Route.toFunction(ReconciliationRoutes(dir).route))
      uri      = s"http://127.0.0.1:${binding.localAddress.getPort}/settlements/$date/reconciliation"
      upload   = HttpEntity(ContentTypes.`text/csv(UTF-8)`, Generator.settlement(count))
      response <- Http().singleRequest(HttpRequest(HttpMethods.POST, Uri(uri), entity = upload))
      last     <- lines(response).runWith(Sink.last) // only the summary is kept, not every result
      _        <- binding.unbind()
    yield
      assertEquals(response.status, StatusCodes.OK)
      val expected = Generator.expectedSummary(count)
      assertEquals(summaryOf(last).hcursor.get[Long]("matched").toOption, Some(expected.matched))
      assertEquals(summaryOf(last).hcursor.get[Long]("missingInLedger").toOption, Some(expected.missingInLedger))
  }

  test("a client that sends the whole upload before reading the response still gets the full result") {
    // The response is about 3x the upload: if the server reconciled straight from the upload, a client
    // that only reads after sending everything would fill the network buffers and both sides would
    // wait for each other until the idle timeout.
    val count = 200_000L // about 5 MB of upload
    val dir   = Files.createTempDirectory("half-duplex")
    for
      _        <- Generator.ledger(count).runWith(FileIO.toPath(dir.resolve(s"ledger-$date.csv")))
      body     <- Generator.settlement(count).runFold(ByteString.empty)(_ ++ _)
      binding  <- Http().newServerAt("127.0.0.1", 0).bind(Route.toFunction(ReconciliationRoutes(dir).route))
      response <- Future(blocking(sendWholeRequestThenRead(binding.localAddress.getPort, body)))
      _        <- binding.unbind()
    yield
      assert(response.startsWith("HTTP/1.1 200"), response.take(200))
      assert(response.contains("\"end\":true"), response.takeRight(500))
  }

  /** Writes the whole request, body included, before reading a single byte of the response, like a
    * half-duplex HTTP/1.1 client (e.g. Python's requests).
    */
  private def sendWholeRequestThenRead(port: Int, body: ByteString): String =
    val socket = new Socket("127.0.0.1", port)
    try
      socket.setSoTimeout(60_000)
      val head =
        s"POST /settlements/$date/reconciliation HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: text/csv\r\n" +
          s"Content-Length: ${body.length}\r\nConnection: close\r\n\r\n"
      val out = socket.getOutputStream
      out.write(head.getBytes(UTF_8))
      out.write(body.toArray)
      out.flush()
      new String(socket.getInputStream.readAllBytes(), UTF_8)
    finally socket.close()
