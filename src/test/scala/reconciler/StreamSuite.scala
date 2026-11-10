package reconciler

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.*
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.ByteString

/** A suite with an actor system to run streams on, terminated after the suite. */
trait StreamSuite extends munit.FunSuite:

  given system: ActorSystem       = ActorSystem(getClass.getSimpleName.replace("$", ""))
  given ec: ExecutionContext      = system.dispatcher

  override def afterAll(): Unit =
    Await.result(system.terminate(), 30.seconds)
    super.afterAll()

  def runToSeq[A](source: Source[A, ?]): Future[Seq[A]] = source.runWith(Sink.seq)

  /** A file with the header and the given lines, as one chunk of bytes. */
  def csv(lines: String*): Source[ByteString, NotUsed] =
    Source.single(ByteString((Csv.Header +: lines).map(_ + "\n").mkString))

  def money(raw: String): Money = Money.parse(raw).fold(error => fail(error), identity)
