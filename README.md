# settlement-reconciler

Streaming **settlement reconciliation** in **Scala 3** with **Apache Pekko Streams** and **Pekko HTTP**:
it reconciles an acquirer's settlement file against the company's own ledger, line by line, with
**constant memory**, whatever the size of the files.

> Everything runs locally and for free: JDK 25 and sbt are all you need. No database, no Docker,
> no API keys. The data is synthetic.

## The problem

Every day, the card acquirer (the company that processes card payments for a merchant) sends a
**settlement file**: every transaction it paid out, and for how much. Finance has to **reconcile**
it against the **ledger**, the company's own record of what it sold:

| Outcome | Meaning |
|---|---|
| **matched** | In both files, same amount and currency |
| **discrepancy** | In both files, but the amount or the currency differ |
| **missing_in_settlement** | In our ledger, but the acquirer didn't pay it: money we expected and didn't get |
| **missing_in_ledger** | Paid by the acquirer, but unknown to us: money we can't explain |
| **duplicate** | The same transaction id more than once in a file |
| **rejected** | A line that couldn't be parsed |

Settlement files easily reach millions of lines, so loading them into memory (or into a map to look
transactions up) doesn't scale. This project reconciles them **as streams**.

## How it works: a streaming merge-join

Both files are sorted by `transaction_id`. They are read **at the same time**, in id order, so the
two records of a transaction meet as they go by, like a database merge join. Only the current line
of each file is held in memory, and each result is emitted as soon as it's known.

```mermaid
flowchart LR
    L[ledger file] --> PL[parse + check order]
    S[settlement file] --> PS[parse + check order]
    PL --> SL{Partition}
    PS --> SS{Partition}
    SL -- valid --> MS[MergeSorted<br/>by id, ledger first]
    SS -- valid --> MS
    MS --> P[pair up<br/>statefulMap]
    P --> M((Merge))
    SL -- rejected / duplicate --> M
    SS -- rejected / duplicate --> M
    M --> R[results] --> SUM[summary<br/>at the end]
```

- **Parse + check order** (`Csv`, `Reconciler.checked`): `Framing.delimiter` splits the bytes into
  lines whatever the chunk boundaries; each line becomes a `Record` or a `Rejected` result. A
  repeated id becomes a `Duplicate`, and an id going **backwards** stops the run with an
  `UnsortedInputException` saying where, since the merge-join depends on sorted input.
- **Partition** sends valid records to the merge and issues straight to the output, so a bad line
  never blocks the reconciliation.
- **MergeSorted** interleaves both files by `(transaction id, side)`: a transaction's ledger record
  comes out right before its settlement record.
- **Pair up** (`statefulMap`) holds at most one record: the next one either has the same id (matched
  or discrepancy) or not (the held one is missing on the other side).
- **Summary**: a final `statefulMap` counts every result and emits the totals once both files are done.

Backpressure is end to end: a slow consumer (e.g. an HTTP client reading slowly) slows the file
reads down instead of piling results up.

## API

### `POST /settlements/{date}/reconciliation`

The request body is the acquirer's settlement file for `date` (`YYYY-MM-DD`). The ledger side is the
company's own export, read from `LEDGER_DIR/ledger-{date}.csv`. The response is **streamed as NDJSON**
(`application/x-ndjson`, chunked): one JSON object per result while the files are read, then a summary.

```bash
sbt run    # starts the server on port 8080, with sample-data as LEDGER_DIR

curl -N --data-binary @sample-data/settlement-2026-11-10.csv \
  -H "Content-Type: text/csv" localhost:8080/settlements/2026-11-10/reconciliation
```

```json
{"type":"matched","transactionId":"tx-0001","amount":120.00,"currency":"BRL"}
{"type":"matched","transactionId":"tx-0002","amount":89.90,"currency":"BRL"}
{"type":"discrepancy","transactionId":"tx-0003","ledger":{"amount":45.00,"currency":"BRL"},"settlement":{"amount":40.00,"currency":"BRL"}}
{"type":"missing_in_settlement","transactionId":"tx-0004","amount":300.00,"currency":"BRL"}
{"type":"missing_in_ledger","transactionId":"tx-0005","amount":60.00,"currency":"BRL"}
{"type":"matched","transactionId":"tx-0006","amount":15.50,"currency":"BRL"}
{"type":"duplicate","transactionId":"tx-0006","amount":15.50,"currency":"BRL","side":"settlement","line":7}
{"type":"discrepancy","transactionId":"tx-0007","ledger":{"amount":99.99,"currency":"USD"},"settlement":{"amount":99.99,"currency":"BRL"}}
{"type":"rejected","side":"settlement","line":9,"reason":"invalid amount: abc"}
{"end":true,"summary":{"matched":3,"discrepancies":2,"missingInSettlement":1,"missingInLedger":1,"duplicates":1,"rejected":1,"matchedAmount":225.40,"missingInSettlementAmount":300.00,"missingInLedgerAmount":60.00}}
```

Paired results come out in transaction id order. Duplicates and rejected lines travel on their own
branch, so their position relative to the paired results may vary.

| Status | When |
|---|---|
| `200` | The reconciliation streams; it ends with the `{"end":true,…}` line |
| `400` | Invalid date |
| `404` | No ledger export for that date |

A `200` response **without** the final `end` line was cut short, e.g. by an unsorted file: once a
stream has started, its status can't change anymore.

**File format:** a `transaction_id,amount,currency` header, then one transaction per line
(`tx-0001,120.00,BRL`), sorted by `transaction_id`. Amounts have at most two decimal places and are
handled as integer cents, never as floating point.

## Running locally

Requirements: **JDK 25** and **sbt**.

```bash
sbt test                                   # the whole test suite
sbt run                                    # the HTTP server (LEDGER_DIR=sample-data, PORT=8080)

# Reconcile two local files from the command line (NDJSON on stdout)
sbt "runMain reconciler.Reconcile sample-data/ledger-2026-11-10.csv sample-data/settlement-2026-11-10.csv"

# Generate a large pair of files (here 5 million transactions each, ~120 MB per file), streamed to disk
sbt "runMain reconciler.GenerateSample generated 5000000 2026-11-10"
LEDGER_DIR=generated sbt run
curl -N --data-binary @generated/settlement-2026-11-10.csv -H "Content-Type: text/csv" \
  localhost:8080/settlements/2026-11-10/reconciliation | tail -1
```

The generated files have a known mix of outcomes (every 40th transaction unsettled, every 50th
missing from the ledger, every 97th settled one cent off), so the summary can be checked exactly.

| Variable | Default | Description |
|---|---|---|
| `LEDGER_DIR` | `sample-data` | Directory with the `ledger-{date}.csv` exports |
| `PORT` | `8080` | HTTP port |

## Design decisions

- **Apache Pekko instead of Akka.** Akka moved to the Business Source License in 2022: production use
  needs a commercial license, and new versions are downloaded from Akka's own repository with a
  token. Pekko is the Apache 2.0 fork, on Maven Central, with native Scala 3 support and practically
  the same API (`org.apache.pekko.stream` instead of `akka.stream`), so Source/Flow/Sink, the Graph
  DSL and backpressure work the same. This project uses Pekko 1.7 with Pekko HTTP 1.4, which is
  built against the Pekko 1.x line (Pekko 2.0 has no matching Pekko HTTP release yet).
- **Merge-join over sorted files, not a lookup map.** Building a map of one file costs memory
  proportional to its size. A merge-join holds one line per file, so memory stays flat from 10 lines
  to 10 million. Sorted input is the price, and settlement and ledger exports can be produced sorted
  by id. An unsorted file stops the run with a precise error instead of producing wrong results.
- **Bad lines don't stop the run.** A malformed line or a duplicate id is reported as a result and
  skipped: one broken line in a 2 GB file shouldn't hide every other discrepancy. Only an unsorted
  file, which makes the whole reconciliation meaningless, fails it.
- **Results stream out as they're found.** The HTTP response is NDJSON over chunked transfer
  encoding, so a client sees discrepancies while the files are still being read, and the server
  never builds the full result in memory.
- **No request size limit on this route.** Pekko HTTP limits request bodies to 8 MB by default; the
  settlement body is streamed, never held in memory, so the limit is lifted for this route only
  (`withoutSizeLimit`).
- **Money as integer cents.** Amounts are parsed into an opaque `Money` type backed by `Long`, so
  sums and comparisons are exact.

## Testing

```bash
sbt test
```

- **`MoneySpec` / `CsvSpec`:** amount parsing, CSV lines split across arbitrary chunk boundaries,
  CRLF, blank lines, a missing final newline, and every kind of rejected line with its reason.
- **`ReconcilerSpec`:** every outcome, files with missing transactions at the start, middle and end,
  an empty file, duplicates and rejected lines alongside the reconciliation, paired results in id
  order, an unsorted file failing with its line, the summary, and the sample files.
- **`LargeFileSpec`:** 500,000 generated transactions per file reconciled to exactly the expected
  summary, and an **endless** pair of files reconciled lazily (taking 5 results ends the run), which
  would hang if any stage collected its input.
- **`ReconciliationRoutesSpec`:** the NDJSON response and its summary, 400 and 404, and, over real
  HTTP, a ~12 MB streamed settlement upload (above the default 8 MB limit) reconciled in full.

## Tech stack

Scala 3.10 · JDK 25 · Apache Pekko Streams 1.7 · Pekko HTTP 1.4 · circe · munit · sbt · GitHub
Actions · Dependabot (sbt and GitHub Actions)

## License

[MIT](LICENSE)
