package models

import javax.inject.Inject
import play.api.libs.json.{Json, OFormat}
import play.modules.reactivemongo.ReactiveMongoApi
import reactivemongo.api.bson.collection.BSONCollection
import reactivemongo.api.bson.{BSONDocument, BSONDocumentReader, BSONDocumentWriter}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

case class Users(uid: Int, recs: List[Movies])
case class Users1(uid: Int, recs: List[MoviesWithName])

object Users {
  implicit val userFormat: OFormat[Users] = Json.format[Users]

  implicit object RateMoviesHandler extends BSONDocumentWriter[Users] with BSONDocumentReader[Users] {
    def writeTry(t: Users) = Success(BSONDocument(
      "uid" -> t.uid,
      "recs" -> t.recs
    ))

    def readDocument(doc: BSONDocument) = for {
      uid <- doc.getAsTry[Int]("uid")
      recs = doc.getAsOpt[List[Movies]]("recs").toList.flatten
    } yield Users(uid, recs)
  }
}
object Users1 {
  implicit val user1Format: OFormat[Users1] = Json.format[Users1]

  implicit object RateMoviesHandler extends BSONDocumentWriter[Users1] with BSONDocumentReader[Users1] {
    def writeTry(t: Users1) = Success(BSONDocument(
      "uid" -> t.uid,
      "recs" -> t.recs
    ))

    def readDocument(doc: BSONDocument) = for {
      uid <- doc.getAsTry[Int]("uid")
      recs = doc.getAsOpt[List[MoviesWithName]]("recs").toList.flatten
    } yield Users1(uid, recs)
  }
}

class userRepository @Inject()(
                                implicit ec: ExecutionContext,
                                reactiveMongoApi: ReactiveMongoApi) {

  import reactivemongo.play.json.compat,
  compat.json2bson._

  private def userCollection: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection[BSONCollection]("UserRecs"))

  private def movieCollection: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection[BSONCollection]("Movie"))


  def getAll: Future[Seq[Users]] =
    userCollection.flatMap(_.find(BSONDocument.empty).
      cursor[Users]().collect[Seq](100))

  def getMovie(uid: Int): Future[Option[Users]] = {
    userCollection.flatMap(_.find(BSONDocument("uid" -> uid)).one[Users])
  }

  def getMovieGMN(mid: Int): Future[Option[MovieGMN]] = {
    movieCollection.flatMap(_.find(BSONDocument("mid" -> mid)).one[MovieGMN])
  }

}

