package it.trenzalore.tools.spark.source.reader

import it.trenzalore.tools.spark.source.SourceConfig
import it.trenzalore.tools.utils.logging.Logging
import it.trenzalore.tools.utils.spark.SparkUtils.schemaOf
import org.apache.spark.sql.{ DataFrame, Dataset, SparkSession }
import it.trenzalore.tools.utils.spark.SparkUtils.implicits._

import scala.reflect.runtime.universe.TypeTag

/**
  * CsvReader allows to read a CSV file from a source described in a SourceConfig.
  */
object CsvReader extends SourceReader with Logging {

  /**
    * Reads a CSV file as a DataFrame from a source described in a SourceConfig.
    * 'delimiter' and 'header' have to be provided in the SourceConfig.
    *
    * @param sourceConfig A source description
    * @param spark A SparkSession instance
    * @return The dataframe containing the parsed CSV file
    * @throws IllegalArgumentException If 'delimiter' or 'header' are not provided
    */
  override def loadDf(sourceConfig: SourceConfig)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Will read CSV dataframe according to the following configuration : $sourceConfig")

    if (sourceConfig.delimiter.isEmpty || sourceConfig.header.isEmpty)
      throw new IllegalArgumentException("'delimiter' and 'header' should be provided to read CSV")

    spark
      .read
      .options(sourceConfig.readOptions)
      .option("delimiter", sourceConfig.delimiter.get)
      .option("header", sourceConfig.header.get)
      .csv(sourceConfig.path)
  }

  /**
    * Reads a CSV file as a Dataset from a source described in a SourceConfig.
    * 'delimiter' and 'header' have to be provided in the SourceConfig.
    * <br/>
    * <br/>
    * Contrary to the original Spark behavior which keeps fields present in the source but not present in the
    * case class, thus potentially leading to some unexpected behavior, only fields present in the case class
    * will be present in the internals.
    *
    * @param sourceConfig A source description
    * @param spark A SparkSession instance
    * @return The dataset containing the parsed CSV file
    * @throws IllegalArgumentException If 'delimiter' or 'header' are not provided
    */
  override def loadDs[T <: Product: TypeTag](sourceConfig: SourceConfig)(implicit spark: SparkSession): Dataset[T] = {
    logger.info(s"Will read CSV dataset according to the following configuration : $sourceConfig")

    if (sourceConfig.delimiter.isEmpty || sourceConfig.header.isEmpty)
      throw new IllegalArgumentException("'delimiter' and 'header' should be provided to read CSV")

    spark
      .read
      .options(sourceConfig.readOptions)
      .option("delimiter", sourceConfig.delimiter.get)
      .option("header", sourceConfig.header.get)
      .schema(schemaOf[T])
      .csv(sourceConfig.path)
      .to[T]
  }

}
