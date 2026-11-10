package reconciler

import scala.concurrent.duration.*
import org.apache.pekko.stream.scaladsl.Sink

/** Files much larger than anything a test would hold in memory, generated lazily line by line. */
class LargeFileSpec extends StreamSuite:

  override val munitTimeout: Duration = 2.minutes

  test("reconciles 500,000 transactions per file to exactly the expected summary") {
    val count = 500_000L
    Reconciler.reconcile(Generator.ledger(count), Generator.settlement(count)).runWith(Sink.last).map { last =>
      assertEquals(last, Output.End(Generator.expectedSummary(count)))
    }
  }

  test("an endless pair of files is reconciled lazily: taking 5 results ends the run") {
    // If any stage collected its input, this would never finish.
    runToSeq(Reconciler.results(Generator.ledger(Long.MaxValue), Generator.settlement(Long.MaxValue)).take(5)).map {
      results => assertEquals(results.size, 5)
    }
  }
