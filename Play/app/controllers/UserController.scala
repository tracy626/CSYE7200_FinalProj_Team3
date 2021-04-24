package controllers

import io.swagger.annotations.{Api, ApiOperation, ApiParam, ApiResponse, ApiResponses}
import javax.inject.Inject
import models.{Users, userRepository}
import models.Users.userFormat
import play.api.libs.json.Json
import play.api.mvc.{AbstractController, ControllerComponents}

import scala.concurrent.ExecutionContext.Implicits.global

@Api(value = "/movies")
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
  def getMovies(@ApiParam(value = "The User ID") uid: Int) =
    Action.async { req =>
      movieRepo.getMovie(uid).map {
        case Some(movies) => Ok(Json.toJson(movies))
        case None    => Ok("Genre Not Found.")
      }.recover{ case t: Throwable =>
        Ok("ERROR: " + t.getMessage)
      }
    }
}

