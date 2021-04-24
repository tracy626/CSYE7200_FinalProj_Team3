package controllers

import javax.inject.Inject
import io.swagger.annotations._
import models.{RateMovies, rateRepository}
import play.api.libs.json.Json
import play.api.mvc.{AbstractController, ControllerComponents}

import scala.concurrent.ExecutionContext.Implicits.global

@Api(value = "/movies")
class RateController @Inject()(
                                cc: ControllerComponents,
                                movieRepo: rateRepository) extends AbstractController(cc) {
  @ApiOperation(
    value = "Find all Movies in Most Rating List",
    response = classOf[RateMovies],
    responseContainer = "List"
  )
  def getAllMovies = Action.async {
    movieRepo.getAll.map{ movies =>
      Ok(Json.toJson(movies))
    }.recover{ case t: Throwable =>
      Ok("ERROR: " + t.getMessage)
    }
  }

  @ApiOperation(
    value = "Get a Movie",
    response = classOf[RateMovies]
  )
  @ApiResponses(Array(
    new ApiResponse(code = 404, message = "Movie not found")
  )
  )
  def getMovies(@ApiParam(value = "The id of the Movie to fetch") movieId: Int) =
    Action.async { req =>
      movieRepo.getMovie(movieId).map {
        case Some(movie) => Ok(Json.toJson(movie))
        case None    => Ok("Movie Not Found.")
      }.recover{ case t: Throwable =>
        Ok("ERROR: " + t.getMessage)
      }
    }
}
