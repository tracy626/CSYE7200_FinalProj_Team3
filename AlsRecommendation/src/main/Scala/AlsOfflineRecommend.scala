import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS

//only need ratings data
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Long)

case class MongoConfig(uri: String, db: String)

// Define benchmark recommendation objects
case class Recommendation(mid: Int, score: Double)

// Define recs
case class UserRecs(uid: Int, recs: Seq[Recommendation])

//...
case class MovieRecs(mid: Int, recd: Seq[Recommendation])
//...

object AlsOfflineRecommend {
  // define tables and invariants
  val MONGODB_RATING_COLLECTION = "Rating"

  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"

  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/csye7200",
      "mongo.db" -> "csye7200"
    )

    // create sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommend")

    // create SparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    //load data
    val ratingRDD = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.uid, rating.mid, rating.score)) //transform to rdd，cut timestamp
      .filter(_._1 < 30800) //decrease users amount
      .cache()

    //filter distinct uid and mid
    val userRDD = ratingRDD.map(_._1).distinct()
    val movieRDD = ratingRDD.map(_._2).distinct()

    //train
    val trainData = ratingRDD.map(x => Rating(x._1, x._2, x._3))

    val (rank, iterations, lambda) = (150, 5, 0.1)
    val model = ALS.train(trainData, rank, iterations, lambda)

    // Calculate the Cartesian product of user and movie to get an empty score matrix
    val userMovies = userRDD.cartesian(movieRDD);
    //predict ratings
    val preRatings = model.predict(userMovies)
    val userRecs = preRatings
      .filter(_.rating > 0) //score>0
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {
        case (uid, recs) => UserRecs(uid, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()

    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    sparkSession.stop()
  }
}