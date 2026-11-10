package reconciler

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.SourceShape
import org.apache.pekko.stream.scaladsl.{Flow, GraphDSL, Merge, MergeSorted, Partition, Source}
import org.apache.pekko.util.ByteString

/** Reconciles a ledger file against a settlement file, both sorted by transaction id, as a stream.
  *
  * It's a merge-join: both files are read at the same time, in transaction id order, and the two
  * records of a transaction meet as they go by. Only the current line of each file is held, so files
  * of any size are reconciled with constant memory, and each result is emitted as soon as it's known.
  *
  * {{{
  *  ledger bytes ──► parse + check order ──► Partition ─ valid ───► MergeSorted ──► pair up ──► Merge ──► results
  *                                                     └ rejected/duplicate ─────────────────────┘ ▲
  *  settlement   ──► parse + check order ──► Partition ─ valid ───┘                                │
  *                                                     └ rejected/duplicate ───────────────────────┘
  * }}}
  */
object Reconciler:

  /** A valid record and the file it comes from. */
  private final case class Tagged(side: Side, record: Record)

  /** Transaction id, then ledger before settlement: a transaction's two records leave the merge
    * next to each other, the ledger one first.
    */
  private given Ordering[Tagged] = Ordering.by((tagged: Tagged) => (tagged.record.transactionId, tagged.side.ordinal))

  /** Every result, then a summary once both files have been read. A failed run never emits the
    * summary.
    */
  def reconcile(ledger: Source[ByteString, ?], settlement: Source[ByteString, ?]): Source[Output, NotUsed] =
    summarized(results(ledger, settlement))

  /** Each result as a line, then the summary of all of them, only if `results` completes without error. */
  private[reconciler] def summarized(results: Source[Result, ?]): Source[Output, NotUsed] =
    results
      .map(Option(_))
      .concat(Source.single(None)) // the end marker: only reached when upstream completes without error
      .statefulMap[Summary, Output](() => Summary.empty)(
        {
          case (summary, Some(result)) => (summary.add(result), Output.Line(result))
          case (summary, None)         => (summary, Output.End(summary))
        },
        // statefulMap also calls this when upstream fails, so the summary can't be emitted here.
        _ => None
      )
      .mapMaterializedValue(_ => NotUsed)

  /** One result per transaction (and per unusable line), in transaction id order for the matched
    * ones. Fails with [[UnsortedInputException]] if a file isn't sorted.
    */
  def results(ledger: Source[ByteString, ?], settlement: Source[ByteString, ?]): Source[Result, NotUsed] =
    Source.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits.*

      val ledgerLines     = builder.add(checked(Side.Ledger))
      val settlementLines = builder.add(checked(Side.Settlement))
      val splitLedger     = builder.add(Partition[Either[Result, Tagged]](2, line => if line.isRight then 0 else 1))
      val splitSettlement = builder.add(Partition[Either[Result, Tagged]](2, line => if line.isRight then 0 else 1))
      val ledgerValid     = builder.add(valid)
      val settlementValid = builder.add(valid)
      val ledgerIssues    = builder.add(issues)
      val settlementIssues = builder.add(issues)
      val merge           = builder.add(MergeSorted[Tagged]())
      val pairUp          = builder.add(pairs)
      val out             = builder.add(Merge[Result](3))

      ledger ~> ledgerLines ~> splitLedger
      settlement ~> settlementLines ~> splitSettlement

      splitLedger.out(0) ~> ledgerValid ~> merge.in0
      splitSettlement.out(0) ~> settlementValid ~> merge.in1
      merge.out ~> pairUp ~> out.in(0)

      splitLedger.out(1) ~> ledgerIssues ~> out.in(1)
      splitSettlement.out(1) ~> settlementIssues ~> out.in(2)

      SourceShape(out.out)
    })

  private def checked(side: Side): Flow[ByteString, Either[Result, Tagged], NotUsed] =
    ordered(side).map(_.map(record => Tagged(side, record)))

  /** Parses a file and checks its order: a repeated transaction id becomes a `Duplicate` (only the
    * first one is reconciled) and an id going backwards stops the run, since the merge-join can only
    * pair up records of sorted files.
    */
  private[reconciler] def ordered(side: Side): Flow[ByteString, Either[Result, Record], NotUsed] =
    Csv
      .records(side)
      .statefulMap[Option[String], Either[Result, Record]](() => None)(
        {
          case (previous, Left(issue)) => (previous, Left(issue))
          case (previous @ Some(previousId), Right((record, line))) if record.transactionId == previousId =>
            (previous, Left(Result.Duplicate(side, line, record)))
          case (Some(previousId), Right((record, line))) if record.transactionId < previousId =>
            throw UnsortedInputException(side, line, record.transactionId, previousId)
          case (_, Right((record, _))) => (Some(record.transactionId), Right(record))
        },
        _ => None
      )

  private val valid: Flow[Either[Result, Tagged], Tagged, NotUsed] =
    Flow[Either[Result, Tagged]].collect { case Right(tagged) => tagged }

  private val issues: Flow[Either[Result, Tagged], Result, NotUsed] =
    Flow[Either[Result, Tagged]].collect { case Left(issue) => issue }

  /** Pairs up the records coming out of the merge: two in a row with the same id are the same
    * transaction (ledger, then settlement); one on its own is missing on the other side.
    */
  private val pairs: Flow[Tagged, Result, NotUsed] =
    Flow[Tagged]
      .statefulMap[Option[Tagged], Option[Result]](() => None)(
        {
          case (None, current) => (Some(current), None)
          case (Some(pending), current) if pending.record.transactionId == current.record.transactionId =>
            (None, Some(compare(pending.record, current.record)))
          case (Some(pending), current) => (Some(current), Some(missing(pending)))
        },
        pending => pending.map(tagged => Some(missing(tagged)))
      )
      .collect { case Some(result) => result }

  private def compare(ledger: Record, settlement: Record): Result =
    if ledger.amount == settlement.amount && ledger.currency == settlement.currency then Result.Matched(ledger)
    else Result.Discrepancy(ledger, settlement)

  private def missing(tagged: Tagged): Result = tagged.side match
    case Side.Ledger     => Result.MissingInSettlement(tagged.record)
    case Side.Settlement => Result.MissingInLedger(tagged.record)
