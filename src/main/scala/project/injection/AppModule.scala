package project.injection

import project.config.JobConfig
import com.google.inject.{AbstractModule, Inject, Provides}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

import scala.util.Try

class AppModule extends AbstractModule {

  override def configure(): Unit = {}

  @Provides
  def providesConfig(): Config = ConfigFactory.load()

  @Provides
  @Inject
  def providesJobConfigTry(config: Config): Try[JobConfig] = JobConfig(config)

  @Provides
  def providesSparkSession(): SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()
      .newSession()

}
