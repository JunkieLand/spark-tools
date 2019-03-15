package it.trenzalore.tools.spark.source

import it.trenzalore.tools.spark.source.WriteStrategy.{ Append, ErrorIfExists, Ignore, Overwrite, OverwritePartitions, OverwriteWhenSuccessful }
import it.trenzalore.tools.spark.source.reader.SourceReader
import it.trenzalore.tools.spark.source.writer.SourceWriter
import it.trenzalore.tools.utils.logging.Logging
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.{ DataFrame, Dataset, SparkSession }

import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag
import scala.util.Random

/**
  * SourceIO is the data source abstraction allowing to read, save or delete any data source described in a
  * SourceConfig.
  *
  * @param sourceName The name of the data source. Only used in logs.
  * @param sourceConfig A data source description allowing to read, save or delete it.
  */
class SourceIO(sourceName: String, sourceConfig: SourceConfig) extends Logging {

  /**
    * Reads any data source described in a SourceConfig as a dataset.
    * <br/>
    * <br/>
    * Contrary to the original Spark behavior which keeps fields present in the source but not present in the
    * case class, thus potentially leading to some unexpected behavior, only fields present in the case class
    * will be present in the internals.
    *
    * @param spark A SparkSession instance
    * @tparam T
    * @return The dataset containing the source
    */
  def loadDs[T <: Product: TypeTag]()(implicit spark: SparkSession): Dataset[T] = {
    logger.info(s"Loading source '$sourceName' with configuration '$sourceConfig'")
    SourceReader.getReader(sourceConfig.format).loadDs[T](sourceConfig)
  }

  /**
    * Reads any data source described in a SourceConfig as a dataframe.
    *
    * @param spark A SparkSession instance
    * @tparam T
    * @return The dataset containing the source
    */
  def loadDf()(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Loading source '$sourceName' with configuration '$sourceConfig'")
    SourceReader.getReader(sourceConfig.format).loadDf(sourceConfig)
  }

  /**
    * Save a dataset as described in a SourceConfig. Note that setting 'saveMode' is mandatory to save a dataset.
    * <br/>
    * <br/>
    * If 'createExternalTable' is set to true, a external table will be created or updated after writing.
    *
    * @param ds The dataset to save
    * @param spark A SparkSession instance
    * @param fs A Hadoop FileSystem
    * @tparam T
    * @throws IllegalArgumentException If 'saveMode' is not provided
    */
  def save[T](ds: Dataset[T])(implicit spark: SparkSession, fs: FileSystem): Unit = {
    logger.info(s"Saving source '$sourceName' with configuration '$sourceConfig'")

    if (sourceConfig.writeStrategy.isEmpty)
      throw new IllegalArgumentException("'saveMode' should be provided to save a dataset")

    sourceConfig.writeStrategy.get match {
      case OverwriteWhenSuccessful ⇒ saveWithOverwriteWhenSuccessful(ds)
      case OverwritePartitions     ⇒ saveWithOverwritePartitions(ds)
      case _                       ⇒ SourceWriter.getWriter(sourceConfig.format).save(ds, sourceConfig)
    }

    if (sourceConfig.createExternalTable)
      createOrUpdateTable()
  }

  /**
    * Delete the source directory and drop the external table if it exists.
    *
    * @param fs A Hadoop FileSystem
    */
  def delete()(implicit spark: SparkSession, fs: FileSystem): Any = {
    logger.info(s"Deleting directory ${sourceConfig.path} for source '$sourceName'")
    fs.delete(new Path(sourceConfig.path), true)
    sourceConfig.table.foreach(dropTable)
  }

  private def saveWithOverwriteWhenSuccessful[T](ds: Dataset[T])(implicit fs: FileSystem): Unit = {
    saveWithIntermediateTempDir(ds, replaceDirectory)
  }

  private def saveWithOverwritePartitions[T](ds: Dataset[T])(implicit fs: FileSystem): Unit = {
    if (sourceConfig.partitions.isEmpty)
      throw new IllegalArgumentException("Partitions are required to use OverwritePartitions save mode.")

    if (sourceDirNonEmptyWithNoPartitions)
      saveWithOverwriteWhenSuccessful(ds)
    else
      saveWithIntermediateTempDir(ds, replacePartitions)
  }

  private def saveWithIntermediateTempDir[T](
    ds:           Dataset[T],
    moveStrategy: (Path, Path) ⇒ Unit
  )(implicit fs: FileSystem): Unit = {
    val tmpSourceConfig = sourceConfig.copy(
      path = sourceConfig.path + "_" + Random.nextString(20),
      writeStrategy = Some(Overwrite)
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

  private def delete(path: String)(implicit fs: FileSystem): Boolean = {
    logger.info(s"Deleting directory $path")
    fs.delete(new Path(path), true)
  }

  private def createOrUpdateTable()(implicit spark: SparkSession): Unit = {
    if (sourceConfig.table.isEmpty)
      throw new IllegalArgumentException("'table' should be provided to use 'createExternalTable'")

    val table = sourceConfig.table.get
    val saveMode = sourceConfig.writeStrategy.get
    val tableExists = spark.catalog.tableExists(table)

    if (saveMode == Overwrite || saveMode == OverwriteWhenSuccessful) {
      dropTable(table)
      createTable(table)
      updateTable(table)
    } else if (!tableExists && Vector(OverwritePartitions, Append, Ignore, ErrorIfExists).contains(saveMode)) {
      createTable(table)
      updateTable(table)
    } else if (tableExists && Vector(OverwritePartitions, Append).contains(saveMode)) {
      updateTable(table)
    }
  }

  private def dropTable(table: String)(implicit spark: SparkSession) = {
    val tableIdentifier = spark.sessionState.sqlParser.parseTableIdentifier(table)
    spark.sessionState.catalog.dropTable(name = tableIdentifier, ignoreIfNotExists = true, purge = false)
  }

  private def createTable(table: String)(implicit spark: SparkSession) = {
    spark.catalog.createTable(table, sourceConfig.path, sourceConfig.format.toString.toLowerCase())
  }

  private def updateTable(table: String)(implicit spark: SparkSession) = {
    spark.catalog.refreshTable(table)
    if (sourceConfig.partitions.nonEmpty)
      spark.catalog.recoverPartitions(table)
  }

}

