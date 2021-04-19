import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StatisticsSpec extends AnyFlatSpec with Matchers {

  "Movie" should "handle movie dataset" in {
    val movie = Movie(1, "Scala", "Science")
    movie.mid shouldBe 1
  }

  "Rating" should "handle rating dataset" in {
    val rate = Rating(1, 1, 5, 1033515200000L)
    rate.score shouldBe 5
  }

  "MongoConfig" should "handle mongodb config" in {
    val config = MongoConfig("mongodb://localhost:27017/csye7200", "csye7200")
    config.db shouldBe("csye7200")
  }
}
