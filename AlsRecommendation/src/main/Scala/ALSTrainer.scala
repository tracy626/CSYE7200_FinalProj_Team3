import AlsOfflineRecommend.MONGODB_RATING_COLLECTION
import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

object ALSTrainer {
  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/csye7200",
      "mongo.db" -> "csye7200"
    )
    // create sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ALSTrainer")

    // create SparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // load ratings data
    val ratingRDD = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => Rating(rating.uid, rating.mid, rating.score)) //转换成rdd，并且去掉时间戳
      .cache()

    // randomly cut datasets, as train sets and test sets
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainingRDD = splits(0)
    val testRDD = splits(1)

    //model parameter, print optimal res
    adjustALSParam(trainingRDD, testRDD)

    sparkSession.close()
  }

  //print res
  def adjustALSParam(trainData: RDD[Rating], testData: RDD[Rating]): Unit = {
    val result = for (rank <- Array(50, 100, 150, 200, 250); lambda <- Array(10, 1, 0.1, 0.01, 0.001))
      yield {
        val model = ALS.train(trainData, rank, 5, lambda)
        // calculate rmse
        val rmse = getRMSE(model, testData)
        (rank, lambda, rmse)
      }
    //print
    println(result.minBy(_._3))
  }

  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    val userMovies = data.map(item => (item.user, item.product))
    val predictRating = model.predict(userMovies)
    val real = data.map(item => ((item.user, item.product), item.rating))
    val predict = predictRating.map(item => ((item.user, item.product), item.rating))
    //RMSE
    sqrt(
      real.join(predict).map { case ((uid, mid), (real, pre)) =>
        // -
        val err = real - pre
        err * err
      }.mean()
    )
  }
}