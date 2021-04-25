package controllers

import javax.inject.Inject
import io.swagger.annotations._
import models.GenresTopMovies
import models.GenresTopMovies.gtopFormat
import models.gTopRepository
import play.api.libs.json.Json
import play.api.mvc.{AbstractController, ControllerComponents}

import scala.concurrent.ExecutionContext.Implicits.global

@Api(value = "/Statistics Recommendation")
class GtopController @Inject()(
                                cc: ControllerComponents,
                                movieRepo: gTopRepository) extends AbstractController(cc) {

  @ApiOperation(
    value = "Find all Movies in Genres Recommendation",
    response = classOf[GenresTopMovies],
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
    value = "Get Movies from Chosen Genres",
    response = classOf[GenresTopMovies]
  )
  @ApiResponses(Array(
    new ApiResponse(code = 404, message = "Genre not found")
  )
  )
  def getMovies(@ApiParam(value = "The genre of the Movies to fetch") genre: String) =
    Action.async { req =>
      movieRepo.getMovie(genre).map {
        case Some(movies) => Ok(Json.toJson(movies))
        case None    => Ok("Genre Not Found.")
      }.recover{ case t: Throwable =>
        Ok("ERROR: " + t.getMessage)
      }
    }
}

