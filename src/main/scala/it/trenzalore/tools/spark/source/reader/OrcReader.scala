package it.trenzalore.tools.spark.source.reader

import it.trenzalore.tools.spark.source.SourceConfig
import it.trenzalore.tools.utils.logging.Logging
import it.trenzalore.tools.utils.spark.SparkUtils.implicits._
import org.apache.spark.sql.{ DataFrame, Dataset, SparkSession }

import scala.reflect.runtime.universe.TypeTag

/**
  * OrcReader allows to read a ORC file from a source described in a SourceConfig.
  */
object OrcReader extends SourceReader with Logging {

  /**
    * Reads a ORC file as a DataFrame from a source described in a SourceConfig.
    *
    * @param sourceConfig A source description
    * @param spark A SparkSession instance
    * @return The dataframe containing the parsed ORC file
    */
  override def loadDf(sourceConfig: SourceConfig)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Will read Orc dataframe according to the following configuration : $sourceConfig")

    spark
      .read
      .options(sourceConfig.readOptions)
      .orc(sourceConfig.path)
  }

  /**
    * Reads a ORC file as a Dataset from a source described in a SourceConfig.
    * <br/>
    * <br/>
    * Contrary to the original Spark behavior which keeps fields present in the source but not present in the
    * case class, thus potentially leading to some unexpected behavior, only fields present in the case class
    * will be present in the internals.
    *
    * @param sourceConfig A source description
    * @param spark A SparkSession instance
    * @return The dataset containing the parsed ORC file
    */
  override def loadDs[T <: Product: TypeTag](sourceConfig: SourceConfig)(implicit spark: SparkSession): Dataset[T] = {
    logger.info(s"Will read Orc dataset according to the following configuration : $sourceConfig")

    loadDf(sourceConfig).to[T]
  }

}

