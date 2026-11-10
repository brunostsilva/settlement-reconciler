package reconciler

import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString

class CsvSpec extends StreamSuite:

  private def parse(bytes: Source[ByteString, ?]) = runToSeq(bytes.via(Csv.records(Side.Settlement)))

  test("parses records, numbering lines from 1 and skipping the header and blank lines") {
    val file = Source.single(ByteString(s"${Csv.Header}\r\ntx-1,10.00,BRL\r\n\r\ntx-2,5.5,USD\n"))
    parse(file).map { parsed =>
      assertEquals(
        parsed,
        Seq(
          Right((Record("tx-1", money("10.00"), "BRL"), 2L)),
          Right((Record("tx-2", money("5.50"), "USD"), 4L))
        )
      )
    }
  }

  test("lines split across chunks, and a last line without a newline, are parsed the same") {
    val text  = s"${Csv.Header}\ntx-1,10.00,BRL\ntx-2,20.00,BRL"
    val bytes = Source(text.grouped(3).map(ByteString(_)).toList) // chunks of 3 bytes
    parse(bytes).map { parsed =>
      assertEquals(parsed.collect { case Right((record, _)) => record.transactionId }, Seq("tx-1", "tx-2"))
    }
  }

  test("malformed lines become Rejected results with their line number and reason") {
    val file = csv("tx-1,10.00", ",10.00,BRL", "tx-3,ten,BRL", "tx-4,10.00,brl", "tx-5,-1.00,BRL")
    parse(file).map { parsed =>
      assertEquals(
        parsed,
        Seq(
          Left(Result.Rejected(Side.Settlement, 2, "expected 3 fields, got 2")),
          Left(Result.Rejected(Side.Settlement, 3, "empty transaction_id")),
          Left(Result.Rejected(Side.Settlement, 4, "invalid amount: ten")),
          Left(Result.Rejected(Side.Settlement, 5, "invalid currency: brl")),
          Left(Result.Rejected(Side.Settlement, 6, "invalid amount: -1.00"))
        )
      )
    }
  }

  test("a file without the header parses its first line as a record") {
    parse(Source.single(ByteString("tx-1,10.00,BRL\n"))).map { parsed =>
      assertEquals(parsed, Seq(Right((Record("tx-1", money("10.00"), "BRL"), 1L))))
    }
  }
