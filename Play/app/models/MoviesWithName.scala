package models

import play.api.libs.json.{Json, OFormat}
import reactivemongo.api.bson.{BSONDocument, BSONDocumentReader, BSONDocumentWriter}

import scala.util.Success

case class MoviesWithName(name: String, mid: Int, score: Double)

object MoviesWithName {
  implicit val movieFormat: OFormat[MoviesWithName] = Json.format[MoviesWithName]

  implicit object RateMoviesHandler extends BSONDocumentWriter[MoviesWithName] with BSONDocumentReader[MoviesWithName] {
    def writeTry(t: MoviesWithName) = Success(BSONDocument(
      "name" -> t.name,
      "mid" -> t.mid,
      "score" -> t.score
    ))

    def readDocument(doc: BSONDocument) = for {
      name <- doc.getAsTry[String]("name")
      mid <- doc.getAsTry[Int]("mid")
      score <- doc.getAsTry[Double]("score")
    } yield MoviesWithName(name, mid, score)
  }
}
