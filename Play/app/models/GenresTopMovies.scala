package models

import javax.inject.Inject

import models.GenresTopMovies.gtopFormat

import scala.concurrent.{ExecutionContext, Future}
import play.api.libs.json.{Json, OFormat}
import play.modules.reactivemongo.ReactiveMongoApi
import reactivemongo.api.{Cursor, ReadPreference}
import reactivemongo.api.commands.WriteResult
import reactivemongo.api.bson.{BSONDocument, BSONDocumentReader, BSONDocumentWriter}
import reactivemongo.api.bson.collection.BSONCollection

import scala.util.Success

case class GenresTopMovies(genres: String, recs: List[MoviesWithName])

object GenresTopMovies {
  implicit val gtopFormat: OFormat[GenresTopMovies] = Json.format[GenresTopMovies]

  implicit object RateMoviesHandler extends BSONDocumentWriter[GenresTopMovies] with BSONDocumentReader[GenresTopMovies] {
    def writeTry(t: GenresTopMovies) = Success(BSONDocument(
      "genres" -> t.genres,
      "recs" -> t.recs
    ))

    def readDocument(doc: BSONDocument) = for {
      genres <- doc.getAsTry[String]("genres")
      recs = doc.getAsOpt[List[MoviesWithName]]("recs").toList.flatten
    } yield GenresTopMovies(genres, recs)
  }
}

class gTopRepository @Inject()(
                                implicit ec: ExecutionContext,
                                reactiveMongoApi: ReactiveMongoApi) {

  import reactivemongo.play.json.compat,
  compat.json2bson._

  private def gtopCollection: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection[BSONCollection]("GenresTopMovies"))

  def getAll: Future[Seq[GenresTopMovies]] =
    gtopCollection.flatMap(_.find(BSONDocument.empty).
      cursor[GenresTopMovies]().collect[Seq](100))

  def getMovie(genre: String): Future[Option[GenresTopMovies]] = {
    gtopCollection.flatMap(_.find(BSONDocument("genres" -> genre)).one[GenresTopMovies])
  }

}

