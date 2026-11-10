package reconciler

import scala.annotation.tailrec
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.util.ByteString

/** Turns the bytes of a ledger or settlement file into records, one line at a time.
  *
  * Format: a `transaction_id,amount,currency` header, then one transaction per line, e.g.
  * `tx-0001,120.00,BRL`. Lines are split as the bytes arrive, whatever the chunk boundaries, so a
  * file of any size is read with constant memory.
  */
object Csv:

  val Header = "transaction_id,amount,currency"

  /** Longest accepted line. A longer one is rejected, and never held in memory beyond this size. */
  val MaxLineLength = 1024

  private val CurrencyCode = "[A-Z]{3}".r

  /** Bytes of the line being read; `tooLong` once it has gone past [[MaxLineLength]], after which
    * its bytes are dropped until the next newline.
    */
  private final case class LineState(buffer: ByteString, tooLong: Boolean)

  private val Empty = LineState(ByteString.empty, tooLong = false)

  /** Splits bytes into lines numbered from 1: `Right(line)`, or `Left(reason)` for a line longer than
    * [[MaxLineLength]] (a bad line is reported, it doesn't stop the run). The last line may have no
    * trailing newline.
    */
  val lines: Flow[ByteString, (Either[String, String], Long), NotUsed] =
    Flow[ByteString]
      .statefulMap[LineState, Vector[Either[String, String]]](() => Empty)(
        (state, chunk) => split(state, chunk, Vector.empty),
        state => Option.when(state.tooLong || state.buffer.nonEmpty)(Vector(finish(state)))
      )
      .mapConcat(identity)
      .zipWithIndex
      .map((line, index) => (line, index + 1))

  @tailrec
  private def split(
      state: LineState,
      rest: ByteString,
      lines: Vector[Either[String, String]]
  ): (LineState, Vector[Either[String, String]]) =
    val newline = rest.indexOf('\n'.toByte)
    if newline < 0 then
      if state.tooLong then (state, lines)
      else
        val buffer = state.buffer ++ rest
        if buffer.length > MaxLineLength then (LineState(ByteString.empty, tooLong = true), lines)
        else (LineState(buffer, tooLong = false), lines)
    else
      val line =
        if state.tooLong then finish(state)
        else finish(LineState(state.buffer ++ rest.take(newline), tooLong = false))
      split(Empty, rest.drop(newline + 1), lines :+ line)

  private def finish(state: LineState): Either[String, String] =
    if state.tooLong || state.buffer.length > MaxLineLength then Left(s"line longer than $MaxLineLength bytes")
    else Right(state.buffer.utf8String.stripSuffix("\r"))

  /** Parses every line of a file. The header and blank lines are skipped; a malformed or overlong
    * line becomes a `Rejected` result instead of failing the whole run.
    */
  def records(side: Side): Flow[ByteString, Either[Result, (Record, Long)], NotUsed] =
    lines.collect {
      case (Left(reason), number) => Left(Result.Rejected(side, number, reason))
      case (Right(line), number) if !(number == 1 && line == Header) && line.trim.nonEmpty =>
        parse(side, line, number)
    }

  def parse(side: Side, line: String, number: Long): Either[Result, (Record, Long)] =
    def reject(reason: String): Either[Result, (Record, Long)] = Left(Result.Rejected(side, number, reason))
    line.split(",", -1).map(_.trim) match
      case Array(id, amount, currency) =>
        if id.isEmpty then reject("empty transaction_id")
        else if !CurrencyCode.matches(currency) then reject(s"invalid currency: $currency")
        else Money.parse(amount).fold(reject, money => Right((Record(id, money, currency), number)))
      case fields => reject(s"expected 3 fields, got ${fields.length}")
