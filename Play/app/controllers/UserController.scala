package controllers

import io.swagger.annotations.{Api, ApiOperation, ApiParam, ApiResponse, ApiResponses}
import javax.inject.Inject
import models.{MoviesWithName, Users, Users1, userRepository}
import models.Users.userFormat
import play.api.libs.json.Json
import play.api.mvc.{AbstractController, ControllerComponents}

import scala.concurrent.ExecutionContext.Implicits.global

@Api(value = "/User Preference")
class UserController @Inject()(
                                cc: ControllerComponents,
                                movieRepo: userRepository) extends AbstractController(cc) {

  @ApiOperation(
    value = "Find all Movies in Genres Recommendation",
    response = classOf[Users],
    responseContainer = "List"
  )
  def getAllMovies = Action.async {
    movieRepo.getAll.map{ recs =>
      Ok(Json.toJson(recs))
    }.recover{ case t: Throwable =>
      Ok("ERROR: " + t.getMessage)
    }
  }

  @ApiOperation(
    value = "Get Movies Recommend for User",
    response = classOf[Users]
  )
  @ApiResponses(Array(
    new ApiResponse(code = 404, message = "User not found")
  )
  )
  def getMovies(@ApiParam(value = "The User ID (30000 - 31199)") uid: Int) =
    Action.async { req =>
      movieRepo.getMovie(uid).map {
        case Some(movies) => {
          val recsNew = for (rec <- movies.recs) yield MoviesWithName(
            {
              val name = movieRepo.getMovieGMN(rec.mid).map {
                case Some(movieGMN) => movieGMN.name
              }
              name.toString
            }
            , rec.mid, rec.score)
          val users1 = Users1(movies.uid, recsNew)
          Ok(Json.toJson(users1))
        }
        case None    => Ok("User ID Not Exist.")
      }.recover{ case t: Throwable =>
        Ok("ERROR: " + t.getMessage)
      }
    }
}

