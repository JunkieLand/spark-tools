package it.trenzalore.utils.spark.io.reader

import it.trenzalore.utils.spark.SparkUtils.implicits._
import it.trenzalore.utils.spark.io.SourceConfig
import org.apache.spark.sql.{ DataFrame, Dataset, SparkSession }

import scala.reflect.runtime.universe.TypeTag

object OrcReader extends SourceReader {

  override def loadDf(sourceConfig: SourceConfig)(implicit spark: SparkSession): DataFrame = {
    spark
      .read
      .options(sourceConfig.readOptions)
      .orc(sourceConfig.path)
  }

  override def loadDs[T <: Product: TypeTag](sourceConfig: SourceConfig)(implicit spark: SparkSession): Dataset[T] = {
    loadDf(sourceConfig).to[T]
  }

}

