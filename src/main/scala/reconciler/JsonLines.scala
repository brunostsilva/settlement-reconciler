package reconciler

import io.circe.{Encoder, Json}
import io.circe.syntax.*

/** One JSON object per output, written as NDJSON (one per line). Amounts are decimal numbers. */
object JsonLines:

  private given Encoder[Money] = Encoder.encodeBigDecimal.contramap(_.toBigDecimal)

  def encode(output: Output): String = json(output).noSpaces

  def json(output: Output): Json = output match
    case Output.Line(result) => json(result)
    case Output.End(summary) => Json.obj("end" -> true.asJson, "summary" -> json(summary))

  private def json(result: Result): Json = result match
    case Result.Matched(record) => typed("matched", record)
    case Result.Discrepancy(ledger, settlement) =>
      Json.obj(
        "type"          -> "discrepancy".asJson,
        "transactionId" -> ledger.transactionId.asJson,
        "ledger"        -> Json.obj("amount" -> ledger.amount.asJson, "currency" -> ledger.currency.asJson),
        "settlement"    -> Json.obj("amount" -> settlement.amount.asJson, "currency" -> settlement.currency.asJson)
      )
    case Result.MissingInSettlement(record) => typed("missing_in_settlement", record)
    case Result.MissingInLedger(record)     => typed("missing_in_ledger", record)
    case Result.Duplicate(side, line, record) =>
      typed("duplicate", record).deepMerge(Json.obj("side" -> sideName(side).asJson, "line" -> line.asJson))
    case Result.Rejected(side, line, reason) =>
      Json.obj(
        "type"   -> "rejected".asJson,
        "side"   -> sideName(side).asJson,
        "line"   -> line.asJson,
        "reason" -> reason.asJson
      )

  private def json(summary: Summary): Json =
    Json.obj(
      "matched"                   -> summary.matched.asJson,
      "discrepancies"             -> summary.discrepancies.asJson,
      "missingInSettlement"       -> summary.missingInSettlement.asJson,
      "missingInLedger"           -> summary.missingInLedger.asJson,
      "duplicates"                -> summary.duplicates.asJson,
      "rejected"                  -> summary.rejected.asJson,
      "matchedAmount"             -> summary.matchedAmount.asJson,
      "missingInSettlementAmount" -> summary.missingInSettlementAmount.asJson,
      "missingInLedgerAmount"     -> summary.missingInLedgerAmount.asJson
    )

  private def typed(kind: String, record: Record): Json =
    Json.obj(
      "type"          -> kind.asJson,
      "transactionId" -> record.transactionId.asJson,
      "amount"        -> record.amount.asJson,
      "currency"      -> record.currency.asJson
    )

  private def sideName(side: Side): String = side.toString.toLowerCase
