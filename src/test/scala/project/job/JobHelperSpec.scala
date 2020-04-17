package project.job

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.{FunSpec, Matchers}

class JobHelperSpec extends FunSpec with Matchers with DataFrameSuiteBase {

  import sqlContext.sparkSession

  private val minimalSchema = StructType(
    Seq(
      StructField("item_id", StringType, nullable = false),
      StructField("image", StringType, nullable = true)
    )
  )

  private def createDataFrameWithMinimalSchema(data: Seq[Row]): DataFrame = {
    sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(data),
      minimalSchema
    )
  }

  describe("Job") {
    describe("splitCatalog") {
      it("splits raw catalog correctly into valid and invalid") {
        // arrange
        val input = createDataFrameWithMinimalSchema(
          Seq(
            Row("1", "image1"),
            Row("2", null),
            Row("3", "image3"),
            Row("4", null),
          )
        )
        val helper = new JobHelper
        // act
        val result = helper.splitCatalog(input)
        // assert
        val expectedValidCatalog = createDataFrameWithMinimalSchema(
          Seq(
            Row("1", "image1"),
            Row("3", "image3"),
          )
        )
        val expectedInvalidCatalog = createDataFrameWithMinimalSchema(
          Seq(
            Row("2", null),
            Row("4", null),
          )
        )
        assertDataFrameEquals(result.validCatalog, expectedValidCatalog)
        assertDataFrameEquals(result.invalidCatalog, expectedInvalidCatalog)
      }
    }
  }

}
