package reconciler

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Flow, Framing}
import org.apache.pekko.util.ByteString

/** Turns the bytes of a ledger or settlement file into records, one line at a time.
  *
  * Format: a `transaction_id,amount,currency` header, then one transaction per line, e.g.
  * `tx-0001,120.00,BRL`. Lines are split as the bytes arrive, whatever the chunk boundaries, so a
  * file of any size is read with constant memory.
  */
object Csv:

  val Header = "transaction_id,amount,currency"

  /** Longest accepted line: a longer one fails the run instead of being buffered without limit. */
  val MaxLineLength = 1024

  private val CurrencyCode = "[A-Z]{3}".r

  /** Splits bytes into lines numbered from 1. The last line may have no trailing newline. */
  val lines: Flow[ByteString, (String, Long), NotUsed] =
    Flow[ByteString]
      .via(Framing.delimiter(ByteString("\n"), MaxLineLength, allowTruncation = true))
      .map(_.utf8String.stripSuffix("\r"))
      .zipWithIndex
      .map((line, index) => (line, index + 1))

  /** Parses every line of a file. The header and blank lines are skipped; a malformed line becomes a
    * `Rejected` result instead of failing the whole run.
    */
  def records(side: Side): Flow[ByteString, Either[Result, (Record, Long)], NotUsed] =
    lines.collect {
      case (line, number) if !(number == 1 && line == Header) && line.trim.nonEmpty => parse(side, line, number)
    }

  def parse(side: Side, line: String, number: Long): Either[Result, (Record, Long)] =
    def reject(reason: String): Either[Result, (Record, Long)] = Left(Result.Rejected(side, number, reason))
    line.split(",", -1).map(_.trim) match
      case Array(id, amount, currency) =>
        if id.isEmpty then reject("empty transaction_id")
        else if !CurrencyCode.matches(currency) then reject(s"invalid currency: $currency")
        else Money.parse(amount).fold(reject, money => Right((Record(id, money, currency), number)))
      case fields => reject(s"expected 3 fields, got ${fields.length}")
