package reconciler

class MoneySpec extends munit.FunSuite:

  test("parses amounts with zero, one or two decimal places into cents") {
    assertEquals(Money.parse("10").map(_.cents), Right(1000L))
    assertEquals(Money.parse("10.5").map(_.cents), Right(1050L))
    assertEquals(Money.parse("10.05").map(_.cents), Right(1005L))
    assertEquals(Money.parse("0.00").map(_.cents), Right(0L))
  }

  test("rejects negative, malformed and over-precise amounts") {
    List("-5.00", "abc", "", "10.", ".50", "10.001", "1,000.00").foreach { raw =>
      assert(Money.parse(raw).isLeft, s"expected \"$raw\" to be rejected")
    }
  }

  test("prints as a decimal with two places") {
    assertEquals(Money.fromCents(1005).toBigDecimal.toString, "10.05")
    assertEquals(Money.fromCents(100).toBigDecimal.toString, "1.00")
  }
