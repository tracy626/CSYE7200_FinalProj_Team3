package controllers

import io.swagger.annotations.{Api, ApiOperation, ApiParam, ApiResponse, ApiResponses}
import javax.inject.Inject
import models.{AvgMovies, avgRepository}
import play.api.libs.json.Json
import play.api.mvc.{AbstractController, ControllerComponents}

import scala.concurrent.ExecutionContext.Implicits.global

@Api(value = "/Statistics Recommendation")
class AvgController  @Inject()(
                                cc: ControllerComponents,
                                movieRepo: avgRepository) extends AbstractController(cc) {
  @ApiOperation(
    value = "Find all Movies in Average Score Recommendation List",
    response = classOf[AvgMovies],
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
    value = "Find all Movies with Request Average Score",
    response = classOf[AvgMovies],
    responseContainer = "List"
  )
  def getAvgMovies(@ApiParam(value = "The averrage score of the Movie to fetch") avg: Double) =
    Action.async {
    movieRepo.getAvgMovie(avg).map{ movies =>
      Ok(Json.toJson(movies))
    }.recover{ case t: Throwable =>
      Ok("ERROR: " + t.getMessage)
    }
  }

  @ApiOperation(
    value = "Get a Movie",
    response = classOf[AvgMovies]
  )
  @ApiResponses(Array(
    new ApiResponse(code = 404, message = "Movie not found")
  )
  )
  def getMovies(@ApiParam(value = "The id of the Movie to fetch") movieId: Int) =
    Action.async { req =>
      movieRepo.getMovie(movieId).map {
        case Some(movie) => Ok(Json.toJson(movie))
        case None => Ok("Movie Not Found.")
      }.recover{ case t: Throwable =>
        Ok("ERROR: " + t.getMessage)
      }
    }
}
