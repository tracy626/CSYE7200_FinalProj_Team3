import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.{DataFrame, SparkSession}

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

object DataClean {

  val ratingPath: String = getClass.getResource("rating.csv").toString
  val tagPath: String = getClass.getResource("tag_test.csv").toString

  def main(args: Array[String]): Unit = {
    val config: Map[String, String] = Map("spark.cores" -> "local[*]")

    // create sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoad")

    // create SparkSession
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val ratingDF: DataFrame = sparkSession.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(ratingPath)

    val ratingDFSelected: DataFrame = selectRaingData(ratingDF, 1)
    val file1 = new File("DataClean/src/main/resources/rating1.csv")
    ratingDFSelected.write.format("csv")
      .option("header", "true").save(file1.getAbsolutePath)

    val tagDF: DataFrame = sparkSession.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(tagPath)
    val tagDFNew: DataFrame = convertComma(tagDF, " ")

    val file2 = new File("DataClean/src/main/resources/tag_new.csv")
    tagDFNew.write.format("csv")
      .option("header", "true").save(file2.getAbsolutePath)
  }

  def selectRaingData(dataFrame: DataFrame, num: Int): DataFrame = {
    val colName = dataFrame.columns(0)
    dataFrame.where(dataFrame(colName) <= num)
  }

  def convertComma(dataFrame: DataFrame, separator: String): DataFrame = {
    val colName = dataFrame.columns(2)
    dataFrame.withColumn(colName, regexp_replace(col(colName), ",", separator))
  }

}
