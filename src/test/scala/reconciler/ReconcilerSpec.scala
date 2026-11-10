package reconciler

import java.nio.file.Path
import org.apache.pekko.stream.scaladsl.{FileIO, Sink}

class ReconcilerSpec extends StreamSuite:

  private def record(id: String, amount: String, currency: String = "BRL") = Record(id, money(amount), currency)

  test("matches transactions with the same amount and currency in both files") {
    runToSeq(Reconciler.results(csv("tx-1,10.00,BRL", "tx-2,20.00,BRL"), csv("tx-1,10.00,BRL", "tx-2,20.00,BRL"))).map {
      results => assertEquals(results, Seq(Result.Matched(record("tx-1", "10.00")), Result.Matched(record("tx-2", "20.00"))))
    }
  }

  test("reports a different amount or currency as a discrepancy, with both sides") {
    runToSeq(Reconciler.results(csv("tx-1,10.00,BRL", "tx-2,20.00,USD"), csv("tx-1,9.99,BRL", "tx-2,20.00,BRL"))).map {
      results =>
        assertEquals(
          results,
          Seq(
            Result.Discrepancy(record("tx-1", "10.00"), record("tx-1", "9.99")),
            Result.Discrepancy(record("tx-2", "20.00", "USD"), record("tx-2", "20.00"))
          )
        )
    }
  }

  test("reports transactions present in only one file, at the start, middle and end") {
    val ledger     = csv("tx-1,1.00,BRL", "tx-3,3.00,BRL", "tx-5,5.00,BRL")
    val settlement = csv("tx-2,2.00,BRL", "tx-3,3.00,BRL", "tx-6,6.00,BRL")
    runToSeq(Reconciler.results(ledger, settlement)).map { results =>
      assertEquals(
        results,
        Seq(
          Result.MissingInSettlement(record("tx-1", "1.00")),
          Result.MissingInLedger(record("tx-2", "2.00")),
          Result.Matched(record("tx-3", "3.00")),
          Result.MissingInSettlement(record("tx-5", "5.00")),
          Result.MissingInLedger(record("tx-6", "6.00"))
        )
      )
    }
  }

  test("an empty file on one side makes every transaction of the other side missing") {
    runToSeq(Reconciler.results(csv(), csv("tx-1,1.00,BRL", "tx-2,2.00,BRL"))).map { results =>
      assertEquals(results, Seq(Result.MissingInLedger(record("tx-1", "1.00")), Result.MissingInLedger(record("tx-2", "2.00"))))
    }
  }

  test("duplicates and malformed lines are reported, and the rest is still reconciled") {
    val ledger     = csv("tx-1,1.00,BRL", "tx-1,1.00,BRL", "tx-2,2.00,BRL")
    val settlement = csv("tx-1,1.00,BRL", "not,a,record", "tx-2,2.00,BRL")
    runToSeq(Reconciler.results(ledger, settlement)).map { results =>
      // Issues travel on their own branch of the graph, so only their presence is asserted, not
      // their position relative to the paired results.
      assertEquals(
        results.toSet,
        Set(
          Result.Matched(record("tx-1", "1.00")),
          Result.Matched(record("tx-2", "2.00")),
          Result.Duplicate(Side.Ledger, 3, record("tx-1", "1.00")),
          Result.Rejected(Side.Settlement, 3, "invalid currency: record")
        )
      )
      assertEquals(results.size, 4)
    }
  }

  test("paired results come out in transaction id order") {
    val ids = (1 to 50).map(i => f"tx-$i%03d")
    val ledger     = csv(ids.filter(_.last != '7').map(id => s"$id,1.00,BRL")*)
    val settlement = csv(ids.filter(_.last != '3').map(id => s"$id,1.00,BRL")*)
    runToSeq(Reconciler.results(ledger, settlement)).map { results =>
      val order = results.map {
        case Result.Matched(r)             => r.transactionId
        case Result.MissingInLedger(r)     => r.transactionId
        case Result.MissingInSettlement(r) => r.transactionId
        case other                         => fail(s"unexpected $other")
      }
      assertEquals(order, ids)
    }
  }

  test("an unsorted file stops the run with an error saying where") {
    val result = runToSeq(Reconciler.results(csv("tx-2,2.00,BRL", "tx-1,1.00,BRL"), csv("tx-1,1.00,BRL"))).failed
    result.map {
      case error: UnsortedInputException =>
        assertEquals((error.side, error.line, error.transactionId, error.previousId), (Side.Ledger, 3L, "tx-1", "tx-2"))
      case other => fail(s"expected an UnsortedInputException, got $other")
    }
  }

  test("a run stopped by an unsorted file never emits the summary, so its partial results can be told apart") {
    // tx-2 is in the ledger, but out of order: by the time that's detected, missing_in_ledger(tx-2)
    // may already be out. The missing summary is what marks those results as invalid.
    val ledger     = csv("tx-1,1.00,BRL", "tx-3,3.00,BRL", "tx-2,2.00,BRL")
    val settlement = csv("tx-2,2.00,BRL", "tx-3,3.00,BRL")
    Reconciler
      .reconcile(ledger, settlement)
      .map[Either[Throwable, Output]](Right(_))
      .recover { case error => Left(error) }
      .runWith(Sink.seq)
      .map { outputs =>
        assert(outputs.last.left.exists(_.isInstanceOf[UnsortedInputException]), outputs)
        assert(!outputs.exists(_.exists(_.isInstanceOf[Output.End])), outputs)
      }
  }

  test("reconcile ends with a summary of counts and amounts, after every result") {
    val ledger     = csv("tx-1,10.00,BRL", "tx-2,20.00,BRL", "tx-3,30.00,BRL")
    val settlement = csv("tx-1,10.00,BRL", "tx-2,25.00,BRL", "tx-4,40.00,BRL", "bad")
    runToSeq(Reconciler.reconcile(ledger, settlement)).map { outputs =>
      assertEquals(outputs.count(_.isInstanceOf[Output.Line]), 5)
      assertEquals(
        outputs.last,
        Output.End(
          Summary.empty.copy(
            matched = 1,
            discrepancies = 1,
            missingInSettlement = 1,
            missingInLedger = 1,
            rejected = 1,
            matchedAmount = money("10.00"),
            missingInSettlementAmount = money("30.00"),
            missingInLedgerAmount = money("40.00")
          )
        )
      )
    }
  }

  test("the sample files in sample-data reconcile to the documented summary") {
    val ledger     = FileIO.fromPath(Path.of("sample-data/ledger-2026-11-10.csv"))
    val settlement = FileIO.fromPath(Path.of("sample-data/settlement-2026-11-10.csv"))
    Reconciler.reconcile(ledger, settlement).runWith(Sink.last).map { last =>
      assertEquals(
        last,
        Output.End(
          Summary(
            matched = 3,
            discrepancies = 2,
            missingInSettlement = 1,
            missingInLedger = 1,
            duplicates = 1,
            rejected = 1,
            matchedAmount = money("225.40"),
            missingInSettlementAmount = money("300.00"),
            missingInLedgerAmount = money("60.00")
          )
        )
      )
    }
  }
