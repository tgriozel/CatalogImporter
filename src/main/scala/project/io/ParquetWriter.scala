package project.io

import cats.effect.IO
import com.google.inject.Singleton
import org.apache.spark.sql.{DataFrame, SaveMode}

@Singleton
class ParquetWriter extends {

  def write(url: String, data: DataFrame): IO[Unit] = IO {
    data.write.mode(SaveMode.Overwrite).parquet(url)
  }

}
