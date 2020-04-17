package project.job

import project.config.JobConfig
import project.io.{CsvReader, ParquetWriter}
import project.model.CatalogSplit
import cats.data.EitherT
import cats.effect.IO
import cats.implicits._
import com.google.inject.{Inject, Singleton}
import org.apache.spark.sql.DataFrame

import scala.util.Try

@Singleton
class Job @Inject()(jobConfigTry: Try[JobConfig], jobHelper: JobHelper, reader: CsvReader, writer: ParquetWriter) {

  private def handleJobConfig: EitherT[IO, JobError, JobConfig] =
    EitherT.fromEither(jobConfigTry.toEither.leftMap(t => JobConfigError(t.getMessage)))

  private def handleInputRead(inputPath: String): EitherT[IO, JobError, DataFrame] =
    EitherT(reader.read(inputPath).attempt).leftMap(t => InputReadError(t.getMessage))

  private def splitCatalog(rawCatalog: DataFrame): EitherT[IO, JobError, CatalogSplit] =
    EitherT.pure(jobHelper.splitCatalog(rawCatalog))

  private def handleWrite(write: IO[Unit]): EitherT[IO, JobError, Unit] = {
    EitherT(write.attempt).leftMap(t => OutputWriteError(t.getMessage))
  }

  def splitCatalog: EitherT[IO, JobError, Unit] =
    for {
      jobConfig <- handleJobConfig
      rawCatalog <- handleInputRead(jobConfig.inputPath)
      catalogSplit <- splitCatalog(rawCatalog)
      _ <- handleWrite(writer.write(jobConfig.rawCatalogPath, rawCatalog))
      _ <- handleWrite(writer.write(jobConfig.validCatalogPath, catalogSplit.validCatalog))
      _ <- handleWrite(writer.write(jobConfig.invalidCatalogPath, catalogSplit.invalidCatalog))
    } yield ()

}
