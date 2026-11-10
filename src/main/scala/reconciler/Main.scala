package reconciler

import java.nio.file.{Files, Path}
import java.time.LocalDate
import scala.concurrent.Future
import scala.util.{Failure, Success}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.scaladsl.FileIO

/** `sbt run`: the HTTP server. `LEDGER_DIR` (default `sample-data`) holds the ledger exports,
  * `PORT` (default 8080) the port.
  */
object ReconcilerServer:

  def main(args: Array[String]): Unit =
    given system: ActorSystem = ActorSystem("settlement-reconciler")
    import system.dispatcher

    val ledgerDir = Path.of(sys.env.getOrElse("LEDGER_DIR", "sample-data")).toAbsolutePath
    val port      = sys.env.get("PORT").flatMap(_.toIntOption).getOrElse(8080)

    Http().newServerAt("0.0.0.0", port).bind(Route.toFunction(ReconciliationRoutes(ledgerDir).route)).onComplete {
      case Success(binding) =>
        system.log.info("Listening on {}, reading ledger exports from {}", binding.localAddress, ledgerDir)
      case Failure(error) =>
        system.log.error(error, "Could not start the server")
        system.terminate()
    }

/** `sbt "runMain reconciler.Reconcile <ledger.csv> <settlement.csv>"`: prints the results as NDJSON. */
object Reconcile:

  def main(args: Array[String]): Unit = args match
    case Array(ledger, settlement) =>
      given system: ActorSystem = ActorSystem("reconcile")
      import system.dispatcher
      Reconciler
        .reconcile(FileIO.fromPath(Path.of(ledger)), FileIO.fromPath(Path.of(settlement)))
        .map(JsonLines.encode)
        .runForeach(println)
        .recover { case error => System.err.println(s"reconciliation failed: ${error.getMessage}") }
        .onComplete(_ => system.terminate())
    case _ =>
      System.err.println("usage: Reconcile <ledger.csv> <settlement.csv>")

/** `sbt "runMain reconciler.GenerateSample [dir] [count] [date]"`: writes `ledger-{date}.csv` and
  * `settlement-{date}.csv` with `count` transactions each (default `generated`, 1,000,000, today),
  * streamed to disk, ready for the server or the CLI.
  */
object GenerateSample:

  def main(args: Array[String]): Unit =
    val dir   = Path.of(args.lift(0).getOrElse("generated"))
    val count = args.lift(1).flatMap(_.toLongOption).getOrElse(1_000_000L)
    val date  = args.lift(2).getOrElse(LocalDate.now.toString)
    Files.createDirectories(dir)

    given system: ActorSystem = ActorSystem("generate-sample")
    import system.dispatcher
    val ledgerFile     = dir.resolve(s"ledger-$date.csv")
    val settlementFile = dir.resolve(s"settlement-$date.csv")
    Future
      .sequence(
        List(
          Generator.ledger(count).runWith(FileIO.toPath(ledgerFile)),
          Generator.settlement(count).runWith(FileIO.toPath(settlementFile))
        )
      )
      .onComplete { result =>
        result match
          case Success(_)     => println(s"Wrote $ledgerFile and $settlementFile ($count transactions)")
          case Failure(error) => System.err.println(s"generation failed: ${error.getMessage}")
        system.terminate()
      }
