package reconciler

import java.nio.file.{Files, Path}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.StreamConverters.*
import scala.util.{Try, Using}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.FlowShape
import org.apache.pekko.stream.scaladsl.{FileIO, Flow, GraphDSL, Keep, Partition, Source}
import org.apache.pekko.util.ByteString

/** Reconciles a pair of files split into N shards by transaction id, the shards in parallel.
  *
  * Both files are split with the same function, `hash(transaction id) % N`, so the two records of a
  * transaction always land in the same shard, and a shard is a self-contained reconciliation. It
  * runs in two phases, like a database's Grace hash join:
  *
  *  1. Each file is read once, parsed and checked as usual (rejected and duplicate lines come out
  *     right away, with their line numbers in the original file), and its valid records are written
  *     to one file per shard, keeping their order.
  *  1. The N pairs of shard files are reconciled in parallel by [[Reconciler]], and the summary adds
  *     up every shard's results.
  *
  * Splitting into files rather than into N in-memory streams is what keeps it deadlock-free: a
  * shard's merge-join waits for the next record of both its files, and with in-memory streams that
  * record could be stuck behind records of other shards, which wait for this one to move on. Writing
  * a shard file never waits for anything, so each file is split at its own pace with constant memory.
  *
  * Results from different shards are interleaved, so they're no longer in transaction id order. An
  * unsorted file still stops the run in the first phase, before any shard is reconciled, and the
  * summary is still only emitted by a run that completes. The shard files are deleted when the run
  * ends, however it ends.
  */
object Sharding:

  val MaxShards = 16

  /** The shard of a transaction id, from 0 to `shards - 1`. */
  def shardOf(transactionId: String, shards: Int): Int = Math.floorMod(transactionId.hashCode, shards)

  /** A number of shards from 1 to [[MaxShards]], 1 when it isn't given. */
  def parse(raw: Option[String]): Either[String, Int] = raw match
    case None => Right(1)
    case Some(value) =>
      value.toIntOption.filter(shards => shards >= 1 && shards <= MaxShards).toRight(
        s"invalid shards: $value (expected 1 to $MaxShards)"
      )

  /** Same outputs as [[Reconciler.reconcile]], in another order; 1 shard is a plain reconciliation.
    * The shard files of each run go to a new directory under `tempDir`.
    */
  def reconcile(
      ledger: Source[ByteString, ?],
      settlement: Source[ByteString, ?],
      shards: Int,
      tempDir: Path = Path.of(System.getProperty("java.io.tmpdir"))
  ): Source[Output, NotUsed] =
    require(shards >= 1, s"shards must be at least 1, got $shards")
    if shards == 1 then Reconciler.reconcile(ledger, settlement)
    else
      Source
        .fromMaterializer { (materializer, _) =>
          given ExecutionContext = materializer.executionContext
          val dir                = Files.createTempDirectory(tempDir, "shards-")
          def files(side: Side)  = (0 until shards).map(shard => dir.resolve(s"${side.toString.toLowerCase}-$shard.csv"))
          val ledgerFiles        = files(Side.Ledger)
          val settlementFiles    = files(Side.Settlement)
          val ledgerWritten      = Promise[Unit]()
          val settlementWritten  = Promise[Unit]()

          // Pekko fuses the stages of a stream into one actor, sub-streams included, so on their own
          // these would all run on one thread: `.async` gives each file's split, and each shard's
          // merge-join, an actor of its own, which is what makes them run in parallel.
          val issues = split(ledger, Side.Ledger, ledgerFiles, ledgerWritten).async
            .merge(split(settlement, Side.Settlement, settlementFiles, settlementWritten).async)
          val shardResults = Source.futureSource(ledgerWritten.future.zip(settlementWritten.future).map { _ =>
            Source(0 until shards).flatMapMerge(
              shards,
              shard => Reconciler.results(FileIO.fromPath(ledgerFiles(shard)), FileIO.fromPath(settlementFiles(shard))).async
            )
          })

          // concatLazy: the shards are only read once both files have been split without error.
          Reconciler
            .summarized(issues.concatLazy(shardResults))
            .watchTermination { (notUsed, done) =>
              done.onComplete(_ => deleteRecursively(dir))(using ExecutionContext.parasitic)
              notUsed
            }
        }
        .mapMaterializedValue(_ => NotUsed)

  /** Phase 1 for one file: emits its rejected and duplicate lines, and writes each valid record to
    * the file of its shard. `written` completes once every shard file has been written.
    */
  private def split(file: Source[ByteString, ?], side: Side, shardFiles: IndexedSeq[Path], written: Promise[Unit])(using
      ExecutionContext
  ): Source[Result, NotUsed] =
    val shards = shardFiles.size
    val sinks  = shardFiles.map(path => Flow[Record].map(render).toMat(FileIO.toPath(path))(Keep.right))
    val splitter = GraphDSL.create(sinks) { implicit builder => shardSinks =>
      import GraphDSL.Implicits.*

      val route = builder.add(
        new Partition[Either[Result, Record]](
          shards + 1,
          {
            case Left(_)       => shards // the last output: rejected and duplicate lines
            case Right(record) => shardOf(record.transactionId, shards)
          },
          eagerCancel = true // a response the client stopped reading stops reading the file too
        )
      )
      val issues = builder.add(Flow[Either[Result, Record]].collect { case Left(issue) => issue })

      shardSinks.zipWithIndex.foreach { (sink, shard) =>
        route.out(shard) ~> Flow[Either[Result, Record]].collect { case Right(record) => record } ~> sink
      }
      route.out(shards) ~> issues
      FlowShape(route.in, issues.out)
    }
    file
      .via(Reconciler.ordered(side))
      .viaMat(splitter)(Keep.right)
      .mapMaterializedValue { writes =>
        written.completeWith(Future.sequence(writes).map(_ => ()))
        NotUsed
      }

  /** A record as a line of a shard file, in the same format as the input files (without the header).
    *
    * The amount is written in its shortest form (`1`, `1.5`, `1.05`), so the line is never longer than
    * the one it was parsed from: a line that fit in [[Csv.MaxLineLength]] when the file was split still
    * fits when its shard is read back.
    */
  private def render(record: Record): ByteString =
    val amount = record.amount.toBigDecimal.bigDecimal.stripTrailingZeros.toPlainString
    ByteString(s"${record.transactionId},$amount,${record.currency}\n")

  private def deleteRecursively(dir: Path): Unit =
    Try {
      Using.resource(Files.list(dir))(_.toScala(List)).foreach(Files.deleteIfExists)
      Files.deleteIfExists(dir)
    }
