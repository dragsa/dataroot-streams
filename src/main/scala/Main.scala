import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import java.time.Instant
import spray.json._

object Main {

  case class User(id: Long,
                  email: String,
                  firstName: String,
                  lastName: String,
                  gender: Char,
                  birthDate: Instant)
  case class Location(id: Long,
                      place: String,
                      country: String,
                      city: String,
                      distance: Int)
  case class Visit(id: Long,
                   location: Long,
                   user: Long,
                   visitedAt: Instant,
                   mark: Int)

  private implicit object EpochDateTimeFormat extends RootJsonFormat[Instant] {
    override def write(obj: Instant) = JsNumber(obj.getEpochSecond)
    override def read(json: JsValue): Instant = json match {
      case JsNumber(s) => Instant.ofEpochSecond(s.toLongExact)
      case _           => throw DeserializationException("Error in Instant parsing")
    }
  }

  object TravelsProtocol extends DefaultJsonProtocol {
    implicit val userFormat: RootJsonFormat[User] = jsonFormat6(User)
    implicit val locationFormat: RootJsonFormat[Location] = jsonFormat5(
      Location)
    implicit val visitFormat: RootJsonFormat[Visit] = jsonFormat5(Visit)
  }

  def main(args: Array[String]): Unit = {
    implicit val sys = ActorSystem("akka-stream-patterns")
    implicit val mat = ActorMaterializer()
    implicit val ec = sys.dispatcher

    val jsonRegexp = "([^\\{]*)(\\{.*\\})".r

    def fileParser(file: java.io.File): Future[List[String]] =
      Source
        .fromIterator(() => io.Source.fromFile(file).iter)
//          .map{a => print(a); a}
        .runFold((Nil: List[String], "")) {
          case ((resultList, buffer), char) =>
            buffer match {
              case jsonRegexp(_, jsonString) => {
                println(jsonString.replaceAll("\\s{2,}", " "))
                (resultList :+ jsonString.replaceAll("\\s{2,}", " "), "")
              }
              case _ =>
                (resultList, if (char.isControl) buffer else buffer + char)
            }
        }
        .map {
          case (resultList, _) =>
            println(s"in ${file.getName} found ${resultList.length}")
            resultList
        }

    val future = Source
      .fromIterator(
        () =>
          new java.io.File(
            "/Users/dragsa/Documents/IdeaProjects/dataroot-streams/data_new/")
            .listFiles()
            .toIterator)
      .filter(_.isFile)
      .filter(!_.getName.contains("visits"))
      .mapAsync(2)(fileParser)
      .runWith(Sink.seq)
      .andThen({
        case _ =>
          Source
            .fromIterator(
              () =>
                new java.io.File(
                  "/Users/dragsa/Documents/IdeaProjects/dataroot-streams/data_new/")
                  .listFiles()
                  .toIterator)
            .filter(_.isFile)
            .filter(_.getName.contains("visits"))
            .mapAsync(2)(fileParser)
            .runWith(Sink.seq)
      })

//    Await.result(future, Duration.Inf)
//    sys.terminate
  }
}
