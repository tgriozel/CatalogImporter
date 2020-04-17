package project.config

import com.typesafe.config.Config

import scala.util.Try

case class JobConfig(inputPath: String, rawCatalogPath: String, validCatalogPath: String, invalidCatalogPath: String)

object JobConfig {

  private val JobConfigPrefix = "catalog-importer"

  def apply(config: Config): Try[JobConfig] =
    for {
      inputPath <- Try(config.getString(s"$JobConfigPrefix.input-file-url"))
      rawCatalogPath <- Try(config.getString(s"$JobConfigPrefix.raw-catalog-output-path"))
      validCatalogPath <- Try(config.getString(s"$JobConfigPrefix.valid-items-output-path"))
      invalidCatalogPath <- Try(config.getString(s"$JobConfigPrefix.invalid-items-output-path"))
    } yield JobConfig(inputPath, rawCatalogPath, validCatalogPath, invalidCatalogPath)

}
