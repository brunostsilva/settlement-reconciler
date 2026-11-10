package reconciler

import java.nio.file.{Files, Path}
import java.util.concurrent.{CyclicBarrier, TimeUnit}
import scala.concurrent.{blocking, Future}
import scala.concurrent.duration.*
import scala.util.Using
import org.apache.pekko.stream.scaladsl.{FileIO, Sink, Source}
import org.apache.pekko.util.ByteString

class ShardingSpec extends StreamSuite:

  override val munitTimeout: Duration = 2.minutes

  private def sample(side: String): Source[ByteString, ?] = FileIO.fromPath(Path.of(s"sample-data/$side-2026-11-10.csv"))

  /** Outputs in a fixed order, to compare runs whose results come out interleaved differently. */
  private def sorted(outputs: Seq[Output]): Seq[Output] = outputs.sortBy(_.toString)

  private def isEmpty(dir: Path): Boolean = Using.resource(Files.list(dir))(_.findAny().isEmpty)

  /** The shard files are deleted as the run ends: waits a little for it rather than racing it. */
  private def assertNoShardFilesLeft(tempDir: Path): Future[Unit] = Future(blocking {
    val deadline = System.nanoTime + 5.seconds.toNanos
    while !isEmpty(tempDir) && System.nanoTime < deadline do Thread.sleep(20)
    assert(isEmpty(tempDir), s"shard files left in $tempDir")
  })

  test("transaction ids spread over every shard, none of them left with a small share") {
    val ids = (1L to 1_000L).map(Generator.id)
    for shards <- Seq(2, 4, 16) do
      val sizes = ids.groupBy(Sharding.shardOf(_, shards)).view.mapValues(_.size).toMap
      assertEquals(sizes.keySet, (0 until shards).toSet)
      assert(sizes.values.forall(_ >= ids.size / shards / 2), s"$shards shards: $sizes")
  }

  test("2 and 4 shards give the same results and summary as a single run, line numbers included") {
    // The sample files have every outcome, a duplicate (line 7) and a rejected line (line 9).
    for
      single <- runToSeq(Reconciler.reconcile(sample("ledger"), sample("settlement")))
      two    <- runToSeq(Sharding.reconcile(sample("ledger"), sample("settlement"), 2))
      four   <- runToSeq(Sharding.reconcile(sample("ledger"), sample("settlement"), 4))
    yield
      for sharded <- Seq(two, four) do
        assertEquals(sorted(sharded.init), sorted(single.init))
        assertEquals(sharded.last, single.last)
  }

  test("20,000 generated transactions in 3 shards: the same results as a single run, and the expected summary") {
    val count = 20_000L
    for
      single  <- runToSeq(Reconciler.reconcile(Generator.ledger(count), Generator.settlement(count)))
      sharded <- runToSeq(Sharding.reconcile(Generator.ledger(count), Generator.settlement(count), 3))
    yield
      assertEquals(sorted(sharded.init), sorted(single.init))
      assertEquals(sharded.last, Output.End(Generator.expectedSummary(count)))
  }

  test("both files are split at the same time, each in its own actor") {
    // Each file's first chunk waits for the other's: if both splits ran in the same actor, the first
    // one to arrive would block it, the other could never arrive, and the barrier would time out.
    val barrier = new CyclicBarrier(2)
    def meetingTheOther(file: Source[ByteString, ?]) = file.map { chunk =>
      barrier.await(10, TimeUnit.SECONDS)
      chunk
    }
    val ledger     = meetingTheOther(csv("tx-1,1.00,BRL", "tx-2,2.00,BRL"))
    val settlement = meetingTheOther(csv("tx-1,1.00,BRL", "tx-3,3.00,BRL"))
    runToSeq(Sharding.reconcile(ledger, settlement, 2)).map { outputs =>
      assertEquals(outputs.size, 4) // tx-1, tx-2 and tx-3, then the summary
    }
  }

  test("a line just under the length limit is still accepted when its shard file is read back") {
    // 1,024 bytes with an integer amount: written as "1.00" it would no longer fit when read back.
    val id     = "tx-" + "9" * (Csv.MaxLineLength - "tx-,1,BRL".length)
    val line   = s"$id,1,BRL"
    assertEquals(line.length, Csv.MaxLineLength)
    runToSeq(Sharding.reconcile(csv(line), csv(line), 2)).map { outputs =>
      assertEquals(outputs.init, Seq(Output.Line(Result.Matched(Record(id, money("1"), "BRL")))))
    }
  }

  test("the shard files are deleted once the run completes") {
    val tempDir = Files.createTempDirectory("sharding")
    for
      outputs <- runToSeq(Sharding.reconcile(sample("ledger"), sample("settlement"), 2, tempDir))
      _       <- assertNoShardFilesLeft(tempDir)
    yield assert(outputs.last.isInstanceOf[Output.End], outputs.last)
  }

  test("an unsorted file stops a sharded run before any shard is reconciled: no summary, no files left") {
    val tempDir    = Files.createTempDirectory("sharding")
    val ledger     = csv("tx-1,1.00,BRL", "tx-3,3.00,BRL", "tx-2,2.00,BRL")
    val settlement = csv("tx-1,1.00,BRL", "tx-2,2.00,BRL", "tx-3,3.00,BRL")
    for
      outputs <- Sharding
        .reconcile(ledger, settlement, 2, tempDir)
        .map[Either[Throwable, Output]](Right(_))
        .recover { case error => Left(error) }
        .runWith(Sink.seq)
      _ <- assertNoShardFilesLeft(tempDir)
    yield
      // The files are split before any shard is reconciled, so the error is the only output.
      outputs match
        case Seq(Left(error: UnsortedInputException)) =>
          assertEquals((error.side, error.line, error.transactionId), (Side.Ledger, 4L, "tx-2"))
        case other => fail(s"expected only an UnsortedInputException, got $other")
  }

  test("a single shard is a plain reconciliation, and fewer than one is an error") {
    intercept[IllegalArgumentException](Sharding.reconcile(csv(), csv(), 0))
    runToSeq(Sharding.reconcile(csv("tx-1,1.00,BRL"), csv("tx-1,1.00,BRL"), 1)).map { outputs =>
      assertEquals(outputs.head, Output.Line(Result.Matched(Record("tx-1", money("1.00"), "BRL"))))
    }
  }

  test("the number of shards is parsed from 1 to 16, 1 when it isn't given") {
    assertEquals(Sharding.parse(None), Right(1))
    assertEquals(Sharding.parse(Some("4")), Right(4))
    assertEquals(Sharding.parse(Some("16")), Right(16))
    for invalid <- Seq("0", "17", "-1", "two", "") do
      assert(Sharding.parse(Some(invalid)).isLeft, invalid)
  }
