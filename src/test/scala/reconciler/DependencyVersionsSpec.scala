package reconciler

import org.apache.pekko
import org.apache.pekko.{PekkoVersion, UnsupportedPekkoVersion}

/** Pekko HTTP is released separately from Pekko, and each Pekko HTTP version requires a minimum Pekko
  * version on the same major line (a Pekko HTTP built for Pekko 1.x can't run on Pekko 2.x, even when it
  * happens to compile). Pekko HTTP checks it when `Http()` starts; this makes the check part of the test
  * suite, so a bump of one without the other fails here, with a clear message.
  */
class DependencyVersionsSpec extends munit.FunSuite:

  test("Pekko HTTP accepts the Pekko version on the classpath") {
    val required = pekko.http.Version.supportedPekkoVersion
    try PekkoVersion.require("pekko-http", required)
    catch
      case error: UnsupportedPekkoVersion =>
        fail(s"Pekko HTTP ${pekko.http.Version.current} needs Pekko $required, found ${pekko.Version.current}", error)
  }
