package models

import javax.inject.Inject
import scala.util.Success

import scala.concurrent.{ExecutionContext, Future}
import play.api.libs.json.{Json, OFormat}
import play.modules.reactivemongo.ReactiveMongoApi
import reactivemongo.api.{Cursor, ReadPreference}
import reactivemongo.api.commands.WriteResult
import reactivemongo.api.bson.{BSONDocument, BSONDocumentReader, BSONDocumentWriter}
import reactivemongo.api.bson.collection.BSONCollection

case class RateMovies(name: String, count: Long, mid: Int)

object RateMovies {
  implicit val rateFormat: OFormat[RateMovies] = Json.format[RateMovies]

  implicit object RateMoviesHandler extends BSONDocumentWriter[RateMovies] with BSONDocumentReader[RateMovies] {
    def writeTry(t: RateMovies) = Success(BSONDocument(
      "movie name" -> t.name,
      "rating count" -> t.count,
      "movie id" -> t.mid
    ))

    def readDocument(doc: BSONDocument) = for {
      name <- doc.getAsTry[String]("name")
      count <- doc.getAsTry[Long]("count")
      mid <- doc.getAsTry[Int]("mid")
    } yield RateMovies(name, count, mid)
  }
}

class rateRepository @Inject()(
                                implicit ec: ExecutionContext,
                                reactiveMongoApi: ReactiveMongoApi) {

  import reactivemongo.play.json.compat,
  compat.json2bson._
// combine RateMoreMovies with Movies, get new collection ???
  private def rateCollection: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection[BSONCollection]("RateMoreMovies"))

  def getAll: Future[Seq[RateMovies]] =
    rateCollection.flatMap(_.find(BSONDocument.empty).
      cursor[RateMovies]().collect[Seq](100))

  def getMovie(id: Int): Future[Option[RateMovies]] = {
    rateCollection.flatMap(_.find(BSONDocument("mid" -> id)).one[RateMovies])
  }

}

