import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.text.SimpleDateFormat

import scala.io.Source

/**
 * Movie Dataset
 *
 * mid: 1                                                     Movie ID
 * name: Toy Story (1995)                                     Movie's name
 * genres： Adventure|Animation|Children|Comedy|Fantasy       Movie's genres
 */
case class Movie(mid: Int, name: String, genres: String)

/**
 * Rating Dataset: begin with uid:30000
 *
 * uid: 30000                   User's ID
 * mid: 10                      Movie's ID according to Movie Dataset
 * score: 3                     Rating Score
 * timestamp: 833515200000      Rating's time (1996/5/31  12:56:00 AM in csv file)
 */
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Long)

/**
 * Tag Dataset
 *
 * uid: 18                       User's ID
 * mid: 4141                     Movie's ID according to Movie Dataset
 * tag: Mark Waters              Tags applied to movies by users
 * timestamp: 1240545600000      Tag's time (2009/4/24  6:19:40 PM in csv file)
 */
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Long)

/**
 * Encapsulate the configuration of mongo into a case class
 *
 * @param uri MongoDB connection
 * @param db  MongoDB Database
 */
case class MongoConfig(uri: String, db: String)

object DataLoad {

  // csv local path
  val moviePath: String = getClass.getResource("movie.csv").toString
  val ratingPath: String = getClass.getResource("rating1200.csv").toString
  val tagPath: String = getClass.getResource("tag_new.csv").toString

//  val MOVIE_DATA_PATH = "/Users/tracy626/Documents/Studies/NEU/CSYE7200/GroupProject/CSYE7200_FinalProj_Team3/DataLoad/src/main/resources/movie.csv"
//  val RATING_DATA_PATH = "/Users/tracy626/Documents/Studies/NEU/CSYE7200/GroupProject/CSYE7200_FinalProj_Team3/DataLoad/src/main/resources/rating1200.csv"
//  val TAG_DATA_PATH = "/Users/tracy626/Documents/Studies/NEU/CSYE7200/GroupProject/CSYE7200_FinalProj_Team3/DataLoad/src/main/resources/tag_new.csv"

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"

  def main(args: Array[String]): Unit = {
    // config of dbs
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/csye7200",
      "mongo.db" -> "csye7200"
    )

    // create sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoad")

    // create SparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._

    // load data
    val movieRDD = sparkSession.sparkContext.textFile(moviePath)

    //RDD->DataFrame
    val movieDF = movieRDD.map(
      item => {
        val attr = item.split(",")
        Movie(attr(0).toInt, attr(1).trim, attr(2).trim)
      }
    ).toDF()

    val ratingRDD = sparkSession.sparkContext.textFile(ratingPath)

    //RDD->DataFrame
    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      val fm = new SimpleDateFormat("yyyy/MM/dd")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, fm.parse(attr(3)).getTime)
    }).toDF()

    val tagRDD = sparkSession.sparkContext.textFile(tagPath)

    //tagRDD->DataFrame
    val tagDF = tagRDD.map(item => {
      val attr = item.split(",")
      val fm = new SimpleDateFormat("yyyy/MM/dd")
      Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, fm.parse(attr(3)).getTime)
    }).toDF()

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // save data to MongoDB
    storeDataInMongoDB(movieDF, ratingDF, tagDF)

    //Data preprocessing, add the tag information corresponding to the movie，tag1|tag2|tag3...
    import org.apache.spark.sql.functions._

    /**
     * mid, tags
     * tags: tag1|tag2|tag3...
     */
    val newTag = tagDF.groupBy($"mid")
      .agg(concat_ws("|", collect_set($"tag")).as("tags"))
      .select("mid", "tags")

    // newTag and movie join
    val movieWithTagsDF = movieDF.join(newTag, Seq("mid"), "left")

    sparkSession.stop()
  }

  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    // Create a new mongodb connection接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    // If there is already a corresponding database in mongodb, delete it first
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    // Write DF data to the corresponding mongodb table
    movieDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // Create Index of the data table
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    mongoClient.close()
  }
}
