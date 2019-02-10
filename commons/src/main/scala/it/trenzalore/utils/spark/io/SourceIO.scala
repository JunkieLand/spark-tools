package it.trenzalore.utils.spark.io

import it.trenzalore.utils.logging.Logging
import it.trenzalore.utils.spark.io.reader.SourceReader
import it.trenzalore.utils.spark.io.writer.SourceWriter
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.{ DataFrame, Dataset, SparkSession }

import scala.annotation.tailrec
import scala.collection.mutable
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

  def save[T](ds: Dataset[T])(implicit fs: FileSystem): Unit = {
    logger.info(s"Saving source '$sourceName' with configuration '$sourceConfig'")

    sourceConfig.saveMode.get match {
      case SaveMode.OverwriteWhenSuccessful ⇒ saveWithOverwriteWhenSuccessful(ds)
      case SaveMode.OverwritePartitions if sourceConfig.partitions.isEmpty ⇒ saveWithOverwriteWhenSuccessful(ds)
      case SaveMode.OverwritePartitions if sourceDirNonEmptyWithNoPartitions ⇒ saveWithOverwriteWhenSuccessful(ds)
      case SaveMode.OverwritePartitions if sourceConfig.partitions.nonEmpty ⇒ saveWithOverwritePartitions(ds)
      case _ ⇒ SourceWriter.getWriter(sourceConfig.format).save(ds, sourceConfig)
    }
  }

  private def saveWithOverwriteWhenSuccessful[T](ds: Dataset[T])(implicit fs: FileSystem): Unit = {
    saveWithIntermediateTempDir(ds, replaceDirectory)
  }

  private def saveWithOverwritePartitions[T](ds: Dataset[T])(implicit fs: FileSystem): Unit = {
    saveWithIntermediateTempDir(ds, replacePartitions)
  }

  private def saveWithIntermediateTempDir[T](
    ds:           Dataset[T],
    moveStrategy: (Path, Path) ⇒ Unit
  )(implicit fs: FileSystem): Unit = {
    val tmpSourceConfig = sourceConfig.copy(
      path = sourceConfig.path + "_" + Random.nextString(20),
      saveMode = Some(SaveMode.Overwrite)
    )
    try {
      SourceWriter.getWriter(sourceConfig.format).save(ds, tmpSourceConfig)
      moveStrategy(new Path(tmpSourceConfig.path), new Path(sourceConfig.path))
    } finally {
      delete(tmpSourceConfig.path)
    }
  }

  private def replaceDirectory(fromDir: Path, toDir: Path)(implicit fs: FileSystem): Unit = {
    fs.delete(toDir, true)
    fs.rename(fromDir, toDir)
  }

  private def replacePartitions(fromDir: Path, toDir: Path)(implicit fs: FileSystem): Unit = {
    val iterator = fs.listFiles(fromDir, true)
    val partitionPaths = mutable.Set[Path]()

    while (iterator.hasNext) {
      val fileStatus = iterator.next()
      val path = fileStatus.getPath

      if (fileStatus.isFile && path.getName != "_SUCCESS") {
        partitionPaths += path.getParent
      }
    }

    partitionPaths
      .flatMap { partitionPath ⇒
        getPartitionSubPath(fromDir, partitionPath).map(subPath ⇒ partitionPath -> subPath)
      }.foreach {
        case (fromPartitionPath, fromPartitionSubPath) ⇒
          replaceDirectory(fromPartitionPath, new Path(toDir, fromPartitionSubPath))
      }
  }

  private def getPartitionSubPath(rootDir: Path, fullDir: Path): Option[Path] = {
    @tailrec
    def loop(currentDir: Path, partitionDir: Option[Path] = None): Option[Path] = {
      if (currentDir.depth() == rootDir.depth()) {
        partitionDir
      } else {
        val newCurrentDir = currentDir.getParent
        val newPartitionDir = partitionDir
          .map(p ⇒ new Path(currentDir.getName, p))
          .getOrElse(new Path(currentDir.getName))
        loop(newCurrentDir, Some(newPartitionDir))
      }
    }
    loop(fullDir)
  }

  private def sourceDirNonEmptyWithNoPartitions(implicit fs: FileSystem): Boolean = {
    val sourcePath = new Path(sourceConfig.path)
    val dirExists = fs.exists(sourcePath)
    lazy val fileStatuses = fs.listStatus(sourcePath)
    dirExists && fileStatuses.nonEmpty && fileStatuses.forall(_.isFile)
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

