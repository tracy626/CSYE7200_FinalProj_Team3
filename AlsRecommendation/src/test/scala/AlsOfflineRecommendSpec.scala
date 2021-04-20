import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AlsOfflineRecommendSpec extends AnyFlatSpec with Matchers {
  "MovieRating" should "handle movie rating data" in {
    val movieR: MovieRating = MovieRating(1, 1, 4.5, 1033515200000L)
    movieR.score shouldBe 4.5
  }

  "Recommendation" should handle "recommendation data" in {
    val rec: Recommendation = Recommendation(1, 4.5)
    rec.score shouldBe 4.5
  }

  "UserRecs" should handle "user recommendation data" in {
    val userRec: UserRecs = UserRecs(1, Seq(Recommendation(1, 4.5)))
    userRec.uid shouldBe 1
  }

  "MovieRecs" should handle "movie recommendation data" in {
    val movieRec: MovieRecs = MovieRecs(1, Seq(Recommendation(1, 4.5)))
    movieRec.mid shouldBe 1
  }
}
