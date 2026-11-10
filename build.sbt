ThisBuild / scalaVersion := "3.10.0"
ThisBuild / version      := "0.1.0"

// Pekko HTTP has no final 2.x release yet: 2.0.0-M2 is the latest milestone on Maven Central, built
// against a Pekko 2.0 milestone and accepted by the final Pekko 2.0.0 (see DependencyVersionsSpec).
val pekkoVersion     = "2.0.0"
val pekkoHttpVersion = "2.0.0-M2"
val circeVersion     = "0.14.17"

lazy val root = (project in file("."))
  .settings(
    name := "settlement-reconciler",
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-stream"      % pekkoVersion,
      "org.apache.pekko" %% "pekko-http"        % pekkoHttpVersion,
      "io.circe"         %% "circe-core"        % circeVersion,
      "org.scalameta"    %% "munit"             % "1.3.6"      % Test,
      "io.circe"         %% "circe-parser"      % circeVersion % Test
    ),
    // `sbt run` starts the HTTP server; the CLI and the sample generator are run with runMain.
    Compile / run / mainClass := Some("reconciler.ReconcilerServer"),
    // Run in a forked JVM: the CLI tools exit with a status code, which must end their own JVM
    // (and fail the sbt command) rather than sbt's.
    Compile / run / fork := true,
    // The tests start an actor system and, in one suite, a real HTTP server: run them in a forked
    // JVM so they don't share sbt's.
    Test / fork := true
  )
