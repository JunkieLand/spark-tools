package it.trenzalore.tools.spark.source

import java.io.File
import java.net.URI

import com.renault.hercules.traceability.DataFrameSuiteBase
import it.trenzalore.tools.utils.spark.SparkUtils.implicits._
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, FunSuite, GivenWhenThen, Matchers }

import scala.reflect.runtime.universe.TypeTag

class SourceIOTest extends FunSuite with Matchers with GivenWhenThen with BeforeAndAfterEach with BeforeAndAfterAll with DataFrameSuiteBase {

  import spark.implicits._
  implicit val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    deleteDir("/tmp/sourceiotest")
    dropTestDb()
    createTestDb()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    deleteDir("/tmp/sourceiotest")
    dropTestDb()
    fs.close()
  }

  test("Save mode Overwrite should write dataset in non existing directory") {
    Given("a dataset and a source configuration with Overwrite save mode")
    val dudes = Seq(Dude(name = "John", age = 18), Dude(name = "Neo", age = 42))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withOverwrite",
      format = FileFormat.Json,
      createExternalTable = false,
      saveMode = Some(SaveMode.Overwrite)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    When("saving the dataset")
    sourceIO.save(dudes.toDS)

    Then("the dataset should be readable from the targeted directory")
    val res = sourceIO.loadDs[Dude]().collect()
    res should contain theSameElementsAs dudes
  }

  test("Save mode Overwrite should write dataset in existing directory") {
    Given("a dataset and a source configuration with Overwrite save mode")
    val dudes = Seq(Dude(name = "Rocco", age = 69), Dude(name = "Alicia", age = 14))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withOverwriteOldDudes",
      format = FileFormat.CSV,
      createExternalTable = false,
      header = Some(true),
      delimiter = Some("|"),
      saveMode = Some(SaveMode.Overwrite)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    And("data already present in the target directory")
    val oldDudes = Seq(Dude(name = "John", age = 18), Dude(name = "Neo", age = 42))
    sourceIO.save(oldDudes.toDS())

    When("saving the dataset")
    sourceIO.save(dudes.toDS)

    Then("the dataset should be readable from the targeted directory with only the writen data")
    val res = sourceIO.loadDs[Dude]().collect()
    res should contain theSameElementsAs dudes
  }

  test("Save mode Append should write dataset in non existing directory") {
    Given("a dataset and a source configuration with Append save mode")
    val dudes = Seq(Dude(name = "John", age = 18), Dude(name = "Neo", age = 42))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withAppend",
      format = FileFormat.Parquet,
      createExternalTable = false,
      saveMode = Some(SaveMode.Append)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    When("saving the dataset")
    sourceIO.save(dudes.toDS)

    Then("the dataset should be readable from the targeted directory")
    val res = sourceIO.loadDs[Dude]().collect()
    res should contain theSameElementsAs dudes
  }

  test("Save mode Append should write dataset in existing directory") {
    Given("a dataset and a source configuration with Overwrite save mode")
    val dudes = Seq(Dude(name = "Rocco", age = 69), Dude(name = "Alicia", age = 14))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withAppendOldDudes",
      format = FileFormat.ORC,
      createExternalTable = false,
      saveMode = Some(SaveMode.Append)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    And("data already present in the target directory")
    val oldDudes = Seq(Dude(name = "John", age = 18), Dude(name = "Neo", age = 42))
    sourceIO.save(oldDudes.toDS())

    When("saving the dataset")
    sourceIO.save(dudes.toDS)

    Then("the dataset should be readable from the targeted directory with old and new data")
    val res = sourceIO.loadDs[Dude]().collect()
    res should contain allElementsOf dudes
    res should contain allElementsOf oldDudes
  }

  test("Save mode Ignore should not write dataset in not existing directory") {
    Given("a dataset and a source configuration with Ignore save mode")
    val dudes = Seq(Dude(name = "Rocco", age = 69), Dude(name = "Alicia", age = 14))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withIgnoreDudes",
      format = FileFormat.ORC,
      createExternalTable = false,
      saveMode = Some(SaveMode.Ignore)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    When("saving the dataset")
    sourceIO.save(dudes.toDS)

    Then("the dataset should be readable from the targeted directory with old data")
    val res = sourceIO.loadDs[Dude]().collect()
    res should contain theSameElementsAs dudes
  }

  test("Save mode Ignore should not write dataset in existing directory") {
    Given("a dataset and a source configuration with Ignore save mode")
    val dudes = Seq(Dude(name = "Rocco", age = 69), Dude(name = "Alicia", age = 14))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withIgnoreDudes",
      format = FileFormat.ORC,
      createExternalTable = false,
      saveMode = Some(SaveMode.Ignore)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    And("data already present in the target directory")
    val oldDudes = Seq(Dude(name = "John", age = 18), Dude(name = "Neo", age = 42))
    sourceIO.save(oldDudes.toDS())

    When("saving the dataset")
    sourceIO.save(dudes.toDS)

    Then("the dataset should be readable from the targeted directory with old data")
    val res = sourceIO.loadDs[Dude]().collect()
    res should contain theSameElementsAs oldDudes
  }

  test("Save mode ErrorIfExists should write dataset if not existing directory") {
    Given("a dataset and a source configuration with ErrorIfExists save mode")
    val dudes = Seq(Dude(name = "Rocco", age = 69), Dude(name = "Alicia", age = 14))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withErrorIfNotExistsDudes",
      format = FileFormat.Json,
      createExternalTable = false,
      saveMode = Some(SaveMode.ErrorIfExists)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    When("saving the dataset")
    sourceIO.save(dudes.toDS)

    Then("the dataset should be readable from the targeted directory")
    val res = sourceIO.loadDs[Dude]().collect()
    res should contain theSameElementsAs dudes
  }

  test("Save mode ErrorIfExists should raise error if existing directory") {
    Given("a dataset and a source configuration with Ignore save mode")
    val dudes = Seq(Dude(name = "Rocco", age = 69), Dude(name = "Alicia", age = 14))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withErrorIfExistsDudes",
      format = FileFormat.Json,
      createExternalTable = false,
      saveMode = Some(SaveMode.ErrorIfExists)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    And("data already present in the target directory")
    val oldDudes = Seq(Dude(name = "John", age = 18), Dude(name = "Neo", age = 42))
    sourceIO.save(oldDudes.toDS())

    When("saving the dataset")
    val exception = intercept[AnalysisException](sourceIO.save(dudes.toDS))

    Then("an error should be raised")
    exception.getMessage() should be("path file:/tmp/sourceiotest/withErrorIfExistsDudes already exists.;")
  }

  test("Save mode OverwriteWhenSuccessful should write with intermediate temporary dir if no existing target") {
    Given("a dataset and a source configuration with Overwrite save mode")
    val dudes = Seq(Dude(name = "John", age = 18), Dude(name = "Neo", age = 42))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withOverwriteWhenSuccessful",
      format = FileFormat.Json,
      createExternalTable = false,
      saveMode = Some(SaveMode.OverwriteWhenSuccessful)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    When("saving the dataset")
    sourceIO.save(dudes.toDS)

    Then("the dataset should be readable from the targeted directory")
    val res = sourceIO.loadDs[Dude]().collect()
    res should contain theSameElementsAs dudes
  }

  test("Save mode OverwriteWhenSuccessful should write with intermediate temporary dir if existing target") {
    Given("a dataset and a source configuration with Overwrite save mode")
    val dudes = Seq(Dude(name = "Rocco", age = 69), Dude(name = "Alicia", age = 14))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withOverwriteWhenSuccessfulOldDudes",
      format = FileFormat.Json,
      createExternalTable = false,
      header = Some(true),
      delimiter = Some("|"),
      saveMode = Some(SaveMode.OverwriteWhenSuccessful)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    And("data already present in the target directory")
    val oldDudes = Seq(Dude(name = "John", age = 18), Dude(name = "Neo", age = 42))
    sourceIO.save(oldDudes.toDS())

    When("saving the dataset")
    sourceIO.save(dudes.toDS)

    Then("the dataset should be readable from the targeted directory with only the writen data")
    val res = sourceIO.loadDs[Dude]().collect()
    res should contain theSameElementsAs dudes
  }

  test("Save mode OverwritePartitions should write dataset if no existing target") {
    Given("a dataset and a source configuration with OverwritePartitions save mode")
    val dudes = Seq(Dude(name = "John", age = 18, height = 100), Dude(name = "Neo", age = 42, height = 100))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withOverwritePartitions",
      format = FileFormat.Json,
      createExternalTable = false,
      partitions = Vector("name", "age"),
      saveMode = Some(SaveMode.OverwritePartitions)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    When("saving the dataset")
    sourceIO.save(dudes.toDS)

    Then("the dataset should be readable from the targeted directory")
    val res = sourceIO.loadDs[Dude]().collect()
    res should contain theSameElementsAs dudes
  }

  test("Save mode OverwritePartitions should overwrite only added partitions if existing target") {
    Given("a dataset and a source configuration with OverwritePartitions save mode")
    val dudes = Seq(Dude(name = "Neo", age = 42, height = 100), Dude(name = "Alicia", age = 14, height = 100))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withOverwriteWhenSuccessfulOldDudes",
      format = FileFormat.Json,
      createExternalTable = false,
      partitions = Vector("name", "age"),
      saveMode = Some(SaveMode.OverwritePartitions)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    And("data already present in the target directory")
    val oldDudes = Seq(Dude(name = "John", age = 18, height = 10), Dude(name = "Neo", age = 42, height = 10))
    sourceIO.save(oldDudes.toDS())

    When("saving the dataset")
    sourceIO.save(dudes.toDS)

    Then("the dataset should be readable with old partitions not present in new ones, plus new ones")
    val res = sourceIO.loadDs[Dude]().collect()
    res should contain theSameElementsAs Seq(
      Dude(name = "Neo", age = 42, height = 100),
      Dude(name = "Alicia", age = 14, height = 100),
      Dude(name = "John", age = 18, height = 10)
    )
  }

  test("Save mode OverwritePartitions should raise error if no partitions are set") {
    Given("a dataset and a source configuration with OverwritePartitions save mode and no partitions")
    val dudes = Seq(Dude(name = "John", age = 18, height = 100), Dude(name = "Neo", age = 42, height = 100))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withOverwriteNoPartitions",
      format = FileFormat.Json,
      createExternalTable = false,
      saveMode = Some(SaveMode.OverwritePartitions)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    When("saving the dataset")
    val exception = intercept[IllegalArgumentException](sourceIO.save(dudes.toDS))

    Then("the dataset should be readable from the targeted directory")
    exception.getMessage should be("Partitions are required to use OverwritePartitions save mode.")
  }

  test("Save mode OverwritePartitions should overwrite everything if no partitions are present in target") {
    Given("a dataset and a source configuration with OverwritePartitions save mode")
    val dudes = Seq(Dude(name = "Neo", age = 42, height = 100), Dude(name = "Alicia", age = 14, height = 100))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withOverwriteWhenSuccessfulOldDudes",
      format = FileFormat.Json,
      createExternalTable = false,
      partitions = Vector("name"),
      saveMode = Some(SaveMode.OverwritePartitions)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    And("data without partitions already present in the target directory")
    val oldDudes = Seq(Dude(name = "John", age = 18, height = 10), Dude(name = "Neo", age = 42, height = 10))
    oldDudes.toDS().write.json(sourceConfig.path)

    When("saving the dataset")
    sourceIO.save(dudes.toDS)

    Then("the dataset should be readable with old partitions not present in new ones, plus new ones")
    val res = sourceIO.loadDs[Dude]().collect()
    res should contain theSameElementsAs dudes
    directoryContainsFiles(sourceConfig.path, "json") should be(false)
  }

  test("Save mode Overwrite and createExternalTable should create table if not exists") {
    Given("a dataset and a source configuration with Overwrite save mode and createExternalTable")
    val dudes = Seq(Dude(name = "Neo", age = 42, height = 100), Dude(name = "Alicia", age = 14, height = 100))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withOverwriteNoExistingCreateExternalTable",
      format = FileFormat.Parquet,
      createExternalTable = true,
      table = Some("withOverwriteNoExistingCreateExternalTable"),
      saveMode = Some(SaveMode.Overwrite)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    When("saving the dataset")
    sourceIO.save(dudes.toDS)

    Then("the table should be readable")
    val res = readTable[Dude]("withOverwriteNoExistingCreateExternalTable")
    res should contain theSameElementsAs dudes
  }

  test("Save mode Overwrite and createExternalTable should recreate table if exists") {
    Given("a dataset and a source configuration with Overwrite save mode and createExternalTable")
    val dudes = Seq(Dude(name = "Neo", age = 42, height = 100), Dude(name = "Alicia", age = 14, height = 100))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withOverwriteExistingCreateExternalTable",
      format = FileFormat.Parquet,
      createExternalTable = true,
      table = Some("withOverwriteExistingCreateExternalTable"),
      saveMode = Some(SaveMode.Overwrite)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    And("an existing table")
    val oldDudes = Seq(Dude(name = "John", age = 18, height = 10), Dude(name = "Neo", age = 42, height = 10))
    sourceIO.save(oldDudes.toDS())

    When("saving the dataset")
    sourceIO.save(dudes.toDS)

    Then("the table should be readable")
    val res = readTable[Dude]("withOverwriteExistingCreateExternalTable")
    res should contain theSameElementsAs dudes
  }

  test("Save mode Append and createExternalTable should create table if not exists") {
    Given("a dataset and a source configuration with Append save mode and createExternalTable")
    val dudes = Seq(Dude(name = "Neo", age = 42, height = 100), Dude(name = "Alicia", age = 14, height = 100))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withAppendNoExistingCreateExternalTable",
      format = FileFormat.Parquet,
      createExternalTable = true,
      table = Some("withAppendNoExistingCreateExternalTable"),
      saveMode = Some(SaveMode.Append)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    When("saving the dataset")
    sourceIO.save(dudes.toDS)

    Then("the table should be readable")
    val res = readTable[Dude]("withAppendNoExistingCreateExternalTable")
    res should contain theSameElementsAs dudes
  }

  test("Save mode Append and createExternalTable should refresh table if exists") {
    Given("a dataset and a source configuration with Append save mode and createExternalTable")
    val dudes = Seq(Dude(name = "Neo", age = 42, height = 100), Dude(name = "Alicia", age = 14, height = 100))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withAppendExistingCreateExternalTable",
      format = FileFormat.Parquet,
      createExternalTable = true,
      table = Some("withAppendExistingCreateExternalTable"),
      saveMode = Some(SaveMode.Append)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    And("an existing table")
    val oldDudes = Seq(Dude(name = "John", age = 18, height = 10), Dude(name = "Neo", age = 42, height = 10))
    sourceIO.save(oldDudes.toDS())

    When("saving the dataset")
    sourceIO.save(dudes.toDS)

    Then("the table should be readable")
    val res = readTable[Dude]("withAppendExistingCreateExternalTable")
    res should contain allElementsOf dudes
    res should contain allElementsOf oldDudes
    res.size should be(4)
  }

  test("Save mode Ignore and createExternalTable should create table if not exists") {
    Given("a dataset and a source configuration with Ignore save mode and createExternalTable")
    val dudes = Seq(Dude(name = "Neo", age = 42, height = 100), Dude(name = "Alicia", age = 14, height = 100))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withIgnoreNoExistingCreateExternalTable",
      format = FileFormat.Parquet,
      createExternalTable = true,
      table = Some("withIgnoreNoExistingCreateExternalTable"),
      saveMode = Some(SaveMode.Ignore)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    When("saving the dataset")
    sourceIO.save(dudes.toDS)

    Then("the table should be readable")
    val res = readTable[Dude]("withIgnoreNoExistingCreateExternalTable")
    res should contain theSameElementsAs dudes
  }

  test("Save mode ErrorIfExists and createExternalTable should create table if not exists") {
    Given("a dataset and a source configuration with ErrorIfExists save mode and createExternalTable")
    val dudes = Seq(Dude(name = "Neo", age = 42, height = 100), Dude(name = "Alicia", age = 14, height = 100))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withErrorIfExistsExistingCreateExternalTable",
      format = FileFormat.Parquet,
      createExternalTable = true,
      table = Some("withErrorIfExistsExistingCreateExternalTable"),
      saveMode = Some(SaveMode.ErrorIfExists)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    When("saving the dataset")
    sourceIO.save(dudes.toDS)

    Then("the table should be readable")
    val res = readTable[Dude]("withErrorIfExistsExistingCreateExternalTable")
    res should contain theSameElementsAs dudes
  }

  test("Save mode OverwriteWhenSuccessful and createExternalTable should create table if not exists") {
    Given("a dataset and a source configuration with OverwriteWhenSuccessful save mode and createExternalTable")
    val dudes = Seq(Dude(name = "Neo", age = 42, height = 100), Dude(name = "Alicia", age = 14, height = 100))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withOverwriteWhenSuccessfulNoExistingCreateExternalTable",
      format = FileFormat.Parquet,
      createExternalTable = true,
      table = Some("withOverwriteWhenSuccessfulNoExistingCreateExternalTable"),
      saveMode = Some(SaveMode.OverwriteWhenSuccessful)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    When("saving the dataset")
    sourceIO.save(dudes.toDS)

    Then("the table should be readable")
    val res = readTable[Dude]("withOverwriteWhenSuccessfulNoExistingCreateExternalTable")
    res should contain theSameElementsAs dudes
  }

  test("Save mode OverwriteWhenSuccessful and createExternalTable should recreate table if exists") {
    Given("a dataset and a source configuration with OverwriteWhenSuccessful save mode and createExternalTable")
    val dudes = Seq(Dude(name = "Neo", age = 42, height = 100), Dude(name = "Alicia", age = 14, height = 100))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withOverwriteWhenSuccessfulExistingCreateExternalTable",
      format = FileFormat.Parquet,
      createExternalTable = true,
      table = Some("withOverwriteWhenSuccessfulExistingCreateExternalTable"),
      saveMode = Some(SaveMode.OverwriteWhenSuccessful)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    And("an existing table")
    val oldDudes = Seq(Dude(name = "John", age = 18, height = 10), Dude(name = "Neo", age = 42, height = 10))
    sourceIO.save(oldDudes.toDS())

    When("saving the dataset")
    sourceIO.save(dudes.toDS)

    Then("the table should be readable")
    val res = readTable[Dude]("withOverwriteWhenSuccessfulExistingCreateExternalTable")
    res should contain theSameElementsAs dudes
  }

  test("Save mode OverwritePartitions and createExternalTable should create table if not exists") {
    Given("a dataset and a source configuration with OverwritePartitions save mode and createExternalTable")
    val dudes = Seq(Dude(name = "Neo", age = 42, height = 100), Dude(name = "Alicia", age = 14, height = 100))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withOverwritePartitionsNoExistingCreateExternalTable",
      format = FileFormat.Parquet,
      partitions = Seq("name", "age"),
      createExternalTable = true,
      table = Some("withOverwritePartitionsNoExistingCreateExternalTable"),
      saveMode = Some(SaveMode.OverwritePartitions)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    When("saving the dataset")
    sourceIO.save(dudes.toDS)

    Then("the table should be readable")
    val res = readTable[Dude]("withOverwritePartitionsNoExistingCreateExternalTable")
    res should contain theSameElementsAs dudes
  }

  test("Save mode OverwritePartitions and createExternalTable should refresh table if exists") {
    Given("a dataset and a source configuration with OverwritePartitions save mode and createExternalTable")
    val dudes = Seq(Dude(name = "Neo", age = 42, height = 100), Dude(name = "Alicia", age = 14, height = 100))

    val sourceConfig = SourceConfig(
      path = "/tmp/sourceiotest/withOverwritePartitionsExistingCreateExternalTable",
      format = FileFormat.Parquet,
      partitions = Seq("name", "age"),
      createExternalTable = true,
      table = Some("test.withOverwritePartitionsExistingCreateExternalTable"),
      saveMode = Some(SaveMode.OverwritePartitions)
    )

    val sourceIO = new SourceIO("dude", sourceConfig)

    And("an existing table")
    val oldDudes = Seq(Dude(name = "John", age = 18, height = 10), Dude(name = "Neo", age = 42, height = 10))
    sourceIO.save(oldDudes.toDS())

    When("saving the dataset")
    sourceIO.save(dudes.toDS)

    Then("the table should be readable")
    val res = readTable[Dude]("test.withOverwritePartitionsExistingCreateExternalTable")
    res should contain allElementsOf Seq(
      Dude(name = "Neo", age = 42, height = 100),
      Dude(name = "Alicia", age = 14, height = 100),
      Dude(name = "John", age = 18, height = 10)
    )
  }

  // Test delete and drop table when delete

  private def deleteDir(path: String) = FileUtils.deleteDirectory(new File(path))

  private def directoryContainsFiles(dir: String, fileExtension: String): Boolean = {
    new File(dir).list().exists(_.endsWith(s".$fileExtension"))
  }

  private def readTable[T <: Product: TypeTag](table: String): Seq[T] = {
    spark.read.table(table).to[T].collect()
  }

  private def dropTestDb() = {
    spark.sessionState.catalog.dropDatabase(db = "test", ignoreIfNotExists = true, cascade = true)
  }

  private def createTestDb() = {
    spark.sessionState.catalog.createDatabase(
      CatalogDatabase("test", "", new URI("file:/home/nico/Projects/Perso/spark-tools/commons/spark-warehouse"), Map()),
      true
    )
  }

}

case class Dude(name: String, age: Int, height: Int = 0)