package project.model

import org.apache.spark.sql.DataFrame

case class CatalogSplit(validCatalog: DataFrame, invalidCatalog: DataFrame)
