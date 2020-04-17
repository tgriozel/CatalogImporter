package project

import cats.effect.{ExitCode, IO, IOApp}
import com.typesafe.scalalogging.LazyLogging
import project.injection.AppInjector
import project.job.{Job, JobError}

object Main extends IOApp with LazyLogging {

  override def run(args: List[String]): IO[ExitCode] = {
    AppInjector.instance.getInstance(classOf[Job]).splitCatalog.value.map {
      case Left(error: JobError) =>
        logger.error(error.message)
        ExitCode.Error
      case Right(()) =>
        logger.info("Success")
        ExitCode.Success
    }
  }

}
