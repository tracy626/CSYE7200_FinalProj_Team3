import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Movie(mid: Int, name: String, genres: String)

case class Rating(uid: Int, mid: Int, score: Double, timestamp: Long)

case class MongoConfig(uri: String, db: String)

//basic recommendation object
case class Recommendation(mid: Int, name: String, score: Double)

//genre top10 case class
case class GenresRecommendation(genres: String, recs: Seq[Recommendation])


object StatisticsRecommend {
  //table
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  //new table
  val RATE_MORE_MOVIES = "RateMoreMovies" //history top
  val AVERAGE_MOVIES = "AverageMovies" //average score top
  val GENRES_TOP_MOVIES = "GenresTopMovies" //genre top

  //db connection
  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/csye7200",
      "mongo.db" -> "csye7200"
    )

    //create sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommend")

    //create SparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // load data from mongodb
    val ratingDF = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    //ratings view
    ratingDF.createOrReplaceTempView("ratings")

    //movies view
    movieDF.createOrReplaceTempView("movies")

    //1.receive score top
    val rateMoreMoviesDF = sparkSession.sql("select mid , name ,count(mid) as count from(SELECT r.mid ,m.name FROM ratings r LEFT JOIN movies m ON r.mid=m.mid) group by mid,name")
    //write
    storeInMongoDB(rateMoreMoviesDF, RATE_MORE_MOVIES)


    //2.average scores top
    val averageMoviesDF = sparkSession.sql("select r.mid, m.name, avg(r.score) as avg from ratings r LEFT JOIN movies m ON r.mid=m.mid group by r.mid, m.name")
    storeInMongoDB(averageMoviesDF, AVERAGE_MOVIES)

    //3.genre top
    //all genres
    val genres = List("Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary", "Drama", "Family", "Fantasy", "Foreign", "History", "Horror", "Music", "Mystery", "Romance", "Science", "Tv", "Thriller", "War", "Western")
    //add average score to movie list，inner join
    val movieWithScore = movieDF.join(averageMoviesDF, Seq("mid"))
    //genres to rdd
    val genresRDD = sparkSession.sparkContext.makeRDD(genres)
    //genre top10，cartesian genre and movie
    val genresTopMoviesDF = genresRDD.cartesian(movieWithScore.rdd)
      .filter {
        //contains genre
        case (genre, movieRow) => movieRow.getAs[String]("genres").toLowerCase.contains(genre.toLowerCase)
      }
      .map {
        case (genre, movieRow) => (genre, (movieRow.getAs[Int]("mid"),movieRow.getAs[String]("name"),movieRow.getAs[Double]("avg")))
      }
      .groupByKey()
      .map {
        case (genre, items) => GenresRecommendation(genre, items.toList.sortWith(_._2 > _._2).take(10).map(item => Recommendation(item._1, item._2,item._3)))
      }
      .toDF()
    storeInMongoDB(genresTopMoviesDF, GENRES_TOP_MOVIES)
  }

  def storeInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}