package models

import javax.inject.Inject
import play.api.libs.json.{Json, OFormat}
import play.modules.reactivemongo.ReactiveMongoApi
import reactivemongo.api.bson.collection.BSONCollection
import reactivemongo.api.bson.{BSONDocument, BSONDocumentReader, BSONDocumentWriter}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

case class AvgMovies(avg: Double, mid: Int)

object AvgMovies {
  implicit val avgFormat: OFormat[AvgMovies] = Json.format[AvgMovies]

  implicit object RateMoviesHandler extends BSONDocumentWriter[AvgMovies] with BSONDocumentReader[AvgMovies] {
    def writeTry(t: AvgMovies) = Success(BSONDocument(
      "avg" -> t.avg,
      "mid" -> t.mid
    ))

    def readDocument(doc: BSONDocument) = for {
      avg <- doc.getAsTry[Double]("avg")
      mid <- doc.getAsTry[Int]("mid")
    } yield AvgMovies(avg, mid)
  }
}


class avgRepository @Inject()(
                                implicit ec: ExecutionContext,
                                reactiveMongoApi: ReactiveMongoApi) {

  import reactivemongo.play.json.compat,
  compat.json2bson._

  private def avgCollection: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection[BSONCollection]("AverageMovies"))

  def getAll: Future[Seq[AvgMovies]] =
    avgCollection.flatMap(_.find(BSONDocument.empty).
      cursor[AvgMovies]().collect[Seq](100))

  def getMovie(id: Int): Future[Option[AvgMovies]] = {
    avgCollection.flatMap(_.find(BSONDocument("mid" -> id)).one[AvgMovies])
  }

}