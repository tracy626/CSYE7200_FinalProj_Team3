package models

import play.api.libs.json.{Json, OFormat}
import reactivemongo.api.bson.{BSONDocument, BSONDocumentReader, BSONDocumentWriter}

import scala.util.Success

case class MovieGMN(genres: String, mid: Int, name: String)

object MovieGMN {
  implicit val movieFormat: OFormat[MovieGMN] = Json.format[MovieGMN]

  implicit object RateMoviesHandler extends BSONDocumentWriter[MovieGMN] with BSONDocumentReader[MovieGMN] {
    def writeTry(t: MovieGMN) = Success(BSONDocument(
      "genres" -> t.genres,
      "mid" -> t.mid,
      "name" -> t.name
    ))

    def readDocument(doc: BSONDocument) = for {
      genres <- doc.getAsTry[String]("genres")
      mid <- doc.getAsTry[Int]("mid")
      name <- doc.getAsTry[String]("name")
    } yield MovieGMN(genres, mid, name)
  }
}
