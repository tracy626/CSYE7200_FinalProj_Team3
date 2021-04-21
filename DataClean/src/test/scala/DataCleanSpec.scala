import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataCleanSpec extends AnyFlatSpec with Matchers {

  "Rating" should "handle rating dataset" in {
    val rate: Rating = Rating(1, 1, 4.5, 1033515200000L)
    rate.score shouldBe 4.5
  }

  behavior of "DataClean"
  it should "get rows of ask feature" in {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("DataCleanSpec")
      .getOrCreate()

    val arrayStructData: Seq[Row] = Seq(
      Row(1, 1),
      Row(2, 2),
      Row(2, 1),
      Row(3, 4),
      Row(4, 2),
      Row(4, 3)
    )

    val arrayStructSchema = new StructType().add("uid", IntegerType).add("mid", IntegerType)
    val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayStructData), arrayStructSchema)
    val num = 2
    val dfSelected = DataClean.selectRaingData(df, num)

    dfSelected.count() shouldBe 3
    spark.stop()
  }

  it should "replace ',' to ' ' in tag dataframe" in {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("DataCleanSpec")
      .getOrCreate()

    val arrayStructData: Seq[Row] = Seq(
      Row(1, 1, "a,b,c,d")
    )

    val arrayStructSchema = new StructType()
      .add("uid", IntegerType)
      .add("mid", IntegerType)
      .add("str",StringType)
    val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayStructData), arrayStructSchema)

    val sep = " "
    val dfModified = DataClean.convertComma(df, sep)

    dfModified.first().get(2) shouldBe("a b c d")
    spark.stop()
  }
}
