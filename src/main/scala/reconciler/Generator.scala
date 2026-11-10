package reconciler

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString

/** Deterministic ledger and settlement files of any size, generated lazily line by line, with a
  * known mix of outcomes. Used by `GenerateSample` and by the large-file tests.
  *
  * For transaction i (1 to count), with id `tx-` and i zero-padded to 10 digits (so ids sort as
  * strings):
  *   - every 40th transaction isn't settled (missing in settlement);
  *   - every 50th isn't in the ledger (missing in ledger);
  *   - every 97th is settled for one cent more (discrepancy);
  *   - the rest match.
  */
object Generator:

  def id(i: Long): String = f"tx-$i%010d"

  def inLedger(i: Long): Boolean     = i % 50 != 0
  def inSettlement(i: Long): Boolean = i % 40 != 0

  def ledgerCents(i: Long): Long     = 100 + i % 10_000
  def settlementCents(i: Long): Long = if i % 97 == 0 then ledgerCents(i) + 1 else ledgerCents(i)

  def ledger(count: Long): Source[ByteString, NotUsed]     = file(count, inLedger, ledgerCents)
  def settlement(count: Long): Source[ByteString, NotUsed] = file(count, inSettlement, settlementCents)

  /** The summary a reconciliation of `ledger(count)` against `settlement(count)` must produce. */
  def expectedSummary(count: Long): Summary =
    Iterator.iterate(1L)(_ + 1).takeWhile(_ <= count).foldLeft(Summary.empty) { (summary, i) =>
      val ledgerRecord     = Record(id(i), Money.fromCents(ledgerCents(i)), "BRL")
      val settlementRecord = Record(id(i), Money.fromCents(settlementCents(i)), "BRL")
      (inLedger(i), inSettlement(i)) match
        case (true, true) if ledgerCents(i) == settlementCents(i) => summary.add(Result.Matched(ledgerRecord))
        case (true, true)                                          => summary.add(Result.Discrepancy(ledgerRecord, settlementRecord))
        case (true, false)                                         => summary.add(Result.MissingInSettlement(ledgerRecord))
        case (false, true)                                         => summary.add(Result.MissingInLedger(settlementRecord))
        case (false, false)                                        => summary
    }

  private def file(count: Long, include: Long => Boolean, cents: Long => Long): Source[ByteString, NotUsed] =
    val transactions = Source
      .fromIterator(() => Iterator.iterate(1L)(_ + 1).takeWhile(_ <= count).filter(include))
      .map(i => s"${id(i)},${Money.fromCents(cents(i)).toBigDecimal},BRL")
    (Source.single(Csv.Header) ++ transactions).map(line => ByteString(line + "\n"))
