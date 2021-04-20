import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class DataLoadSpec extends AnyFlatSpec with Matchers {
  "Movie" should "handle movie dataset" in {
    val movie = Movie(1, "Scala", "Science")
    movie.mid shouldBe 1
  }

  "Rating" should "handle rating dataset" in {
    val rate = Rating(1, 1, 5, 1033515200000L)
    rate.score shouldBe 5
  }

  "Tag" should "handle tag dataset" in {
    val tag = Tag(1, 1, "test", 1270545600000L)
    tag.uid shouldBe 1
  }

  "MongoConfig" should "handle mongodb config" in {
    val config = MongoConfig("mongodb://localhost:27017/csye7200_test ", "csye7200_test")
    config.db shouldBe("csye7200_test")
  }

  behavior of "DataLoad"
  it should "store data into mongodb" in {
    // config of dbs
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/csye7200_test",
      "mongo.db" -> "csye7200_test"
    )

    // create sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoad")

    // create SparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._
    val movieData:Seq[Row] = Seq(Row(1, "Toy Story (1995)", "Adventure|Animation|Children|Comedy|Fantasy"))
    val ratingData:Seq[Row] = Seq(Row(30000, 10, 3.0, 833515200000L))
    val tagData:Seq[Row] = Seq(Row(18, 4141,"Mark Waters", 1240545600000L))

    val movieSchema = new StructType()
      .add("uid", IntegerType)
      .add("name", StringType)
      .add("genres", StringType)
    val ratingSchema = new StructType()
      .add("uid", IntegerType)
      .add("mid", IntegerType)
      .add("score", DoubleType)
      .add("timestamp", LongType)
    val tagSchema = new StructType()
      .add("uid", IntegerType)
      .add("mid", IntegerType)
      .add("tag", StringType)
      .add("timestamp", LongType)

    val movieDF = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(movieData), movieSchema)
    val ratingDF = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(ratingData), ratingSchema)
    val tagDF = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(tagData), tagSchema)

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // save data to MongoDB
    DataLoad.storeDataInMongoDB(movieDF, ratingDF, tagDF)

    val ratingReadDF: DataFrame = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", "Rating")
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    ratingReadDF.first().getAs[Int]("uid") shouldBe 30000

    sparkSession.stop()
  }
}
