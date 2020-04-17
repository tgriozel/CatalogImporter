package project.io

import cats.effect.IO
import com.google.inject.{Inject, Singleton}
import org.apache.spark.SparkFiles
import org.apache.spark.sql.{DataFrame, SparkSession}

@Singleton
class CsvReader @Inject() (sparkSession: SparkSession) {

  def read(path: String): IO[DataFrame] = IO {
    sparkSession.sparkContext.addFile(path)
    val fileName = path.split('/').last
    val reader = sparkSession.read.option("header", "true").option("inferSchema", "true")
    reader.csv(SparkFiles.get(fileName))
  }

}
