package models

import play.api.libs.json.{Json, OFormat}
import reactivemongo.api.bson.{BSONDocument, BSONDocumentReader, BSONDocumentWriter}

import scala.util.Success

case class Movies(mid: Int, score: Double)

object Movies {
  implicit val movieFormat: OFormat[Movies] = Json.format[Movies]

  implicit object RateMoviesHandler extends BSONDocumentWriter[Movies] with BSONDocumentReader[Movies] {
    def writeTry(t: Movies) = Success(BSONDocument(
      "mid" -> t.mid,
      "score" -> t.score
    ))

    def readDocument(doc: BSONDocument) = for {
      mid <- doc.getAsTry[Int]("mid")
      score <- doc.getAsTry[Double]("score")
    } yield Movies(mid, score)
  }
}
