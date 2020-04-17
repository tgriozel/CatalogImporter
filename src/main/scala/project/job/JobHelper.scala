package project.job

import project.model.{CatalogSplit, ColumnNames}
import com.google.inject.Singleton
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

@Singleton
class JobHelper {

  def splitCatalog(rawCatalog: DataFrame): CatalogSplit = {
    val validCatalog = rawCatalog.where(col(ColumnNames.imageColumn).isNotNull)
    val invalidCatalog = rawCatalog.where(col(ColumnNames.imageColumn).isNull)
    CatalogSplit(validCatalog, invalidCatalog)
  }

}
