package it.trenzalore.utils.spark.io.reader
import it.trenzalore.utils.spark.SparkUtils.schemaOf
import it.trenzalore.utils.spark.io.SourceConfig
import org.apache.spark.sql.{ DataFrame, Dataset, SparkSession }
import it.trenzalore.utils.spark.SparkUtils.implicits._
import scala.reflect.runtime.universe.TypeTag

object JsonReader extends SourceReader {

  override def loadDf(sourceConfig: SourceConfig)(implicit spark: SparkSession): DataFrame = {
    spark
      .read
      .options(sourceConfig.readOptions)
      .json(sourceConfig.path)
  }

  override def loadDs[T <: Product: TypeTag](sourceConfig: SourceConfig)(implicit spark: SparkSession): Dataset[T] = {
    spark
      .read
      .options(sourceConfig.readOptions)
      .schema(schemaOf[T])
      .json(sourceConfig.path)
      .to[T]
  }

}
