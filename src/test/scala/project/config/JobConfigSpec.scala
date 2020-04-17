package project.config

import com.typesafe.config.{ConfigException, ConfigFactory}
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.TryValues._

import scala.util.Success

class JobConfigSpec extends FunSpec with Matchers {

  describe("JobConfig") {
    describe("apply") {
      it("parses a Config and returns a correct Success(JobConfig)") {
        // arrange
        val url = "url"
        val rawCatalogPath = "raw-catalog-path"
        val validItemsPath = "valid-items-path"
        val invalidItemsPath = "invalid-items-path"
        val config = ConfigFactory.parseString(
          s"""
             |catalog-importer {
             |  input-file-url: $url
             |  raw-catalog-output-path = $rawCatalogPath
             |  valid-items-output-path = $validItemsPath
             |  invalid-items-output-path = $invalidItemsPath
             |}
             |""".stripMargin
        )
        // act
        val result = JobConfig(config)
        // assert
        result.success shouldBe Success(JobConfig(url, rawCatalogPath, validItemsPath, invalidItemsPath))
      }

      it("parses a Config and returns a Failure if the config is not correct") {
        // arrange
        val config = ConfigFactory.parseString(
          s"""
             |catalog-importer {
             |  input-file-url: "url"
             |}
             |""".stripMargin
        )
        // act
        val result = JobConfig(config)
        // assert
        result.failure.exception shouldBe a[ConfigException]
      }
    }
  }

}
