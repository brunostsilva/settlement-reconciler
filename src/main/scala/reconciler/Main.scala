package reconciler

import java.nio.file.{Files, Path}
import java.time.LocalDate
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}
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

/** Runs a command-line task to completion and exits with a status scripts can rely on: 0 on success,
  * 1 on failure (with the error on stderr), after shutting the actor system down.
  */
private def exitWhenDone(task: Future[?])(using system: ActorSystem): Nothing =
  val status = Try(Await.result(task, Duration.Inf)) match
    case Success(_) => 0
    case Failure(error) =>
      System.err.println(s"failed: ${error.getMessage}")
      1
  Await.ready(system.terminate(), 30.seconds)
  sys.exit(status)

/** `sbt "runMain reconciler.Reconcile <ledger.csv> <settlement.csv>"`: prints the results as NDJSON.
  *
  * Exits with 1 if the run fails (e.g. an unsorted file): the lines already printed must then be
  * discarded, and only a run ending with the `{"end":true,...}` line is valid. Exits with 2 on wrong
  * usage.
  */
object Reconcile:

  def main(args: Array[String]): Unit = args match
    case Array(ledger, settlement) =>
      given system: ActorSystem = ActorSystem("reconcile")
      exitWhenDone(
        Reconciler
          .reconcile(FileIO.fromPath(Path.of(ledger)), FileIO.fromPath(Path.of(settlement)))
          .map(JsonLines.encode)
          .runForeach(println)
      )
    case _ =>
      System.err.println("usage: Reconcile <ledger.csv> <settlement.csv>")
      sys.exit(2)

/** `sbt "runMain reconciler.GenerateSample [dir] [count] [date]"`: writes `ledger-{date}.csv` and
  * `settlement-{date}.csv` with `count` transactions each (default `generated`, 1,000,000, today),
  * streamed to disk, ready for the server or the CLI. Exits with 1 if writing fails.
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
    exitWhenDone(
      Future
        .sequence(
          List(
            Generator.ledger(count).runWith(FileIO.toPath(ledgerFile)),
            Generator.settlement(count).runWith(FileIO.toPath(settlementFile))
          )
        )
        .map(_ => println(s"Wrote $ledgerFile and $settlementFile ($count transactions)"))
    )
