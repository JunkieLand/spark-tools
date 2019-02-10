package it.trenzalore.utils.spark.io

import it.trenzalore.utils.logging.Logging
import it.trenzalore.utils.spark.io.reader.SourceReader
import it.trenzalore.utils.spark.io.writer.SourceWriter
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.{ DataFrame, Dataset, SparkSession }

import scala.reflect.runtime.universe.TypeTag
import scala.util.Random

class SourceIO(sourceName: String, sourceConfig: SourceConfig) extends Logging {

  def loadDs[T <: Product: TypeTag]()(implicit spark: SparkSession): Dataset[T] = {
    logger.info(s"Loading source '$sourceName' with configuration '$sourceConfig'")
    SourceReader.getReader(sourceConfig.format).loadDs[T](sourceConfig)
  }

  def loadDf()(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Loading source '$sourceName' with configuration '$sourceConfig'")
    SourceReader.getReader(sourceConfig.format).loadDf(sourceConfig)
  }

  def save[T <: Product: TypeTag](ds: Dataset[T])(implicit fs: FileSystem): Unit = {
    logger.info(s"Saving source '$sourceName' with configuration '$sourceConfig'")

    val writer = SourceWriter.getWriter(sourceConfig.format)

    sourceConfig.saveMode.get match {
      case SaveMode.OverwriteWhenSuccessful ⇒
        val tmpSourceConfig = sourceConfig.copy(
          path = sourceConfig.path + "_" + Random.nextString(20),
          saveMode = Some(SaveMode.Overwrite)
        )
        try {
          writer.save(ds, tmpSourceConfig)
          replaceDir(tmpSourceConfig.path, sourceConfig.path)
        } finally {
          delete(tmpSourceConfig.path)
        }

      case _ ⇒
        writer.save(ds, sourceConfig)
    }
  }

  def replaceDir(fromDir: String, toDir: String)(implicit fs: FileSystem): Unit = {
    fs.delete(new Path(toDir), true)
    fs.rename(new Path(fromDir), new Path(toDir))
  }

  def delete()(implicit fs: FileSystem): Boolean = {
    logger.info(s"Deleting directory ${sourceConfig.path} for source '$sourceName'")
    fs.delete(new Path(sourceConfig.path), true)
  }

  private def delete(path: String)(implicit fs: FileSystem): Boolean = {
    logger.info(s"Deleting directory $path")
    fs.delete(new Path(path), true)
  }

}

