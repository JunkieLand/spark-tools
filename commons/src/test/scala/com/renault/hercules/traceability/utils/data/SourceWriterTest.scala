package com.renault.hercules.traceability.utils.data

import java.io.File

import com.renault.hercules.traceability.DataFrameSuiteBase
import com.typesafe.config.ConfigFactory
import it.trenzalore.utils.SourceWriter
import it.trenzalore.utils.spark.FileFormat.Parquet
import org.apache.commons.io.FileUtils
import org.scalatest.{ BeforeAndAfterAll, FlatSpec, GivenWhenThen, Matchers }

class SourceWriterTest extends FlatSpec with Matchers with GivenWhenThen with BeforeAndAfterAll with DataFrameSuiteBase {

  import spark.implicits._

  val config = ConfigFactory.load()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    FileUtils.deleteDirectory(new File(config.getString("sources.writeTestWithMinOptions.path")))
    FileUtils.deleteDirectory(new File(config.getString("sources.writeTestWithMaxOptions.path")))
  }

  it should "parse source configuration properly" in {
    Given("a source with a write configuration")
    val sourceWriter = new SourceWriter {
      override val sourceName: String = "writeTestWithMaxOptions"
    }

    Then("parameters should be parsed")
    sourceWriter.sourceFormat should be(Parquet)
    sourceWriter.sourcePath should be("/tmp/tracabilite/SourceWriterTest/writeTestWithMaxOptions")
    sourceWriter.database should be("sourceWriterTestDb")
    sourceWriter.table should be("sourceWriterTestTable")
    sourceWriter.options.toList should contain theSameElementsAs Seq("option_name" -> "toto")
  }

  it should "save a dataset with no optinal config parameters" in {
    Given("a simple source writer and a dataset")
    val sourceWriter = new SourceWriter {
      val sourceName: String = "writeTestWithMinOptions"
    }
    val rows = Seq(
      MyRow("a", "b", "c"),
      MyRow("x", "y", "z")
    )
    val ds = spark.createDataset(rows).repartition(1)

    When("writing the dataset without a table")
    sourceWriter.save(ds)

    Then("I should be able to read the written dataset")
    val writtenDs = spark
      .read
      .parquet(config.getString("sources.writeTestWithMinOptions.path"))
      .as[MyRow]
      .collect()
    writtenDs should contain theSameElementsAs rows
  }

  it should "save a dataset with all optinal config parameters" in {
    Given("a simple source writer and a dataset")
    val sourceWriter = new SourceWriter {
      val sourceName: String = "writeTestWithMaxOptions"
    }
    val rows = Seq(
      MyRow("a", "b", "c"),
      MyRow("x", "y", "z")
    )
    val ds = spark.createDataset(rows).repartition(1)

    When("writing the dataset without a table")
    sourceWriter.save(ds)

    Then("I should be able to read the written dataset")
    val writtenDs = spark
      .read
      .parquet(config.getString("sources.writeTestWithMaxOptions.path"))
      .as[MyRow]
      .collect()
    writtenDs should contain theSameElementsAs rows
  }

}

case class MyRow(field_1: String, field_2: String, field_3: String)