package reconciler

/** Amount in integer cents, so money is never a floating-point number. */
opaque type Money = Long

object Money:

  /** Parses a decimal amount with at most two decimal places ("10", "10.5", "10.50"), rejecting
    * negative, malformed or out-of-range values.
    */
  def parse(raw: String): Either[String, Money] =
    raw match
      case Money.Pattern(units, decimals) =>
        val cents = Option(decimals).getOrElse("").padTo(2, '0')
        (units + cents).toLongOption.toRight(s"amount out of range: $raw")
      case _ => Left(s"invalid amount: $raw")

  def fromCents(cents: Long): Money = cents

  private val Pattern = """(\d{1,15})(?:\.(\d{1,2}))?""".r

  extension (money: Money)
    def cents: Long              = money
    def toBigDecimal: BigDecimal = BigDecimal(money, 2)
    def +(other: Money): Money   = money + other

/** Which file a record comes from: our own ledger, or the acquirer's settlement file. */
enum Side:
  case Ledger, Settlement

/** One transaction as listed in a file. Both files are sorted by `transactionId`. */
final case class Record(transactionId: String, amount: Money, currency: String)

/** The outcome for one transaction, or for one line that couldn't be used. */
enum Result:
  /** In both files with the same amount and currency. */
  case Matched(record: Record)

  /** In both files, but the amount or the currency differ. */
  case Discrepancy(ledger: Record, settlement: Record)

  /** In our ledger, but the acquirer didn't settle it: money we expected and didn't get. */
  case MissingInSettlement(record: Record)

  /** Settled by the acquirer, but unknown to our ledger: money we can't explain. */
  case MissingInLedger(record: Record)

  /** A transaction id that appears more than once in the same file (only the first one is reconciled). */
  case Duplicate(side: Side, line: Long, record: Record)

  /** A line that couldn't be parsed: it's reported and skipped instead of failing the whole run. */
  case Rejected(side: Side, line: Long, reason: String)

/** Counts per kind of result, and the totals that matter to finance. */
final case class Summary(
    matched: Long,
    discrepancies: Long,
    missingInSettlement: Long,
    missingInLedger: Long,
    duplicates: Long,
    rejected: Long,
    matchedAmount: Money,
    missingInSettlementAmount: Money,
    missingInLedgerAmount: Money
):
  def add(result: Result): Summary = result match
    case Result.Matched(r)             => copy(matched = matched + 1, matchedAmount = matchedAmount + r.amount)
    case Result.Discrepancy(_, _)      => copy(discrepancies = discrepancies + 1)
    case Result.MissingInSettlement(r) =>
      copy(missingInSettlement = missingInSettlement + 1, missingInSettlementAmount = missingInSettlementAmount + r.amount)
    case Result.MissingInLedger(r) =>
      copy(missingInLedger = missingInLedger + 1, missingInLedgerAmount = missingInLedgerAmount + r.amount)
    case Result.Duplicate(_, _, _) => copy(duplicates = duplicates + 1)
    case Result.Rejected(_, _, _)  => copy(rejected = rejected + 1)

object Summary:
  val empty: Summary = Summary(0, 0, 0, 0, 0, 0, Money.fromCents(0), Money.fromCents(0), Money.fromCents(0))

/** What a reconciliation run emits: one line per result, then the summary once the run completes. */
enum Output:
  case Line(result: Result)
  case End(summary: Summary)

/** Thrown when a file isn't sorted by transaction id: the merge-join depends on it, so the run stops. */
final class UnsortedInputException(val side: Side, val line: Long, val transactionId: String, val previousId: String)
    extends RuntimeException(
      s"${side.toString.toLowerCase} file is not sorted by transaction_id: line $line has \"$transactionId\" " +
        s"after \"$previousId\""
    )
