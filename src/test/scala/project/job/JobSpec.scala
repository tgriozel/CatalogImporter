package project.job

import project.config.JobConfig
import project.io.{CsvReader, ParquetWriter}
import project.model.CatalogSplit
import cats.effect.IO
import org.apache.spark.sql.DataFrame
import org.mockito.Mockito.verify
import org.mockito.captor.ArgCaptor
import org.mockito.scalatest.IdiomaticMockito
import org.scalatest.{FunSpec, Matchers}

import scala.util.{Failure, Success}

class JobSpec extends FunSpec with Matchers with IdiomaticMockito {

  private val jobConfig = JobConfig("inputPath", "rawCatalogPath", "validCatalogPath", "invalidCatalogPath")

  describe("Job") {
    describe("splitCatalog") {
      it("reads from the indicated source, splits catalogs using the helper, and writes them to indicated locations") {
        // arrange
        val (input, validCatalog, invalidCatalog) = (mock[DataFrame], mock[DataFrame], mock[DataFrame])
        val jobHelper = mock[JobHelper]
        jobHelper.splitCatalog(input) returns CatalogSplit(validCatalog, invalidCatalog)
        val csvReader = mock[CsvReader]
        csvReader.read(jobConfig.inputPath) returns IO(input)
        val parquetWriter = mock[ParquetWriter]
        parquetWriter.write(any[String], any[DataFrame]) returns IO(())
        val job = new Job(Success(jobConfig), jobHelper, csvReader, parquetWriter)
        // act
        val _ = job.splitCatalog.value.unsafeRunSync()
        // assert
        val rawCatalogCaptor = ArgCaptor[DataFrame]
        val validCatalogCaptor = ArgCaptor[DataFrame]
        val invalidCatalogCaptor = ArgCaptor[DataFrame]
        verify(parquetWriter).write(eqTo(jobConfig.rawCatalogPath), rawCatalogCaptor.capture)
        verify(parquetWriter).write(eqTo(jobConfig.validCatalogPath), validCatalogCaptor.capture)
        verify(parquetWriter).write(eqTo(jobConfig.invalidCatalogPath), invalidCatalogCaptor.capture)
        rawCatalogCaptor.value shouldBe input
        validCatalogCaptor.value shouldBe validCatalog
        invalidCatalogCaptor.value shouldBe invalidCatalog
      }

      it("returns a JobConfigError if the config was a Failure") {
        // arrange
        val jobConfigTry = Failure(new Exception("config error"))
        val jobHelper = mock[JobHelper]
        val csvReader = mock[CsvReader]
        val parquetWriter = mock[ParquetWriter]
        val job = new Job(jobConfigTry, jobHelper, csvReader, parquetWriter)
        // act
        val result = job.splitCatalog.value.unsafeRunSync()
        // assert
        result shouldBe Left(JobConfigError("config error"))
      }

      it("returns an InputReadError if reading the input fails") {
        // arrange
        val jobHelper = mock[JobHelper]
        val csvReader = mock[CsvReader]
        csvReader.read(jobConfig.inputPath) returns IO.fromTry(Failure(new Exception("read error")))
        val parquetWriter = mock[ParquetWriter]
        val job = new Job(Success(jobConfig), jobHelper, csvReader, parquetWriter)
        // act
        val result = job.splitCatalog.value.unsafeRunSync()
        // assert
        result shouldBe Left(InputReadError("read error"))
      }


      it("returns an OutputWriteError if writing one of the results fails") {
        // arrange
        val (input, validCatalog, invalidCatalog) = (mock[DataFrame], mock[DataFrame], mock[DataFrame])
        val jobHelper = mock[JobHelper]
        jobHelper.splitCatalog(input) returns CatalogSplit(validCatalog, invalidCatalog)
        val csvReader = mock[CsvReader]
        csvReader.read(jobConfig.inputPath) returns IO(input)
        val parquetWriter = mock[ParquetWriter]
        parquetWriter.write(jobConfig.rawCatalogPath, input) returns IO.fromTry(Failure(new Exception("write error")))
        val job = new Job(Success(jobConfig), jobHelper, csvReader, parquetWriter)
        // act
        val result = job.splitCatalog.value.unsafeRunSync()
        // assert
        result shouldBe Left(OutputWriteError("write error"))
      }
    }
  }

}
