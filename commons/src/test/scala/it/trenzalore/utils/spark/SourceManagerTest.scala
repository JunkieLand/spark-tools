package it.trenzalore.utils.spark

import com.typesafe.config.ConfigFactory
import it.trenzalore.utils.spark.io.{ SourceConfig, SourceManager }
import org.apache.spark.sql.SaveMode
import org.scalatest.{ FunSuite, GivenWhenThen, Matchers }

class SourceManagerTest extends FunSuite with Matchers with GivenWhenThen {

  test("source config parser should return a map of source configuration") {
    Given("a source configuration")
    val configStr =
      """
        |sources {
        |  source1 {
        |    path = "source1_path"
        |    format = "orc"
        |  }
        |  source2 {
        |    path = "source2_path"
        |    format = "json"
        |    delimiter = ";"
        |    header = true
        |    partitions = ["part1", "part2"]
        |    table = "some_table"
        |    saveMode = "Overwrite"
        |    createExternalTable = true
        |    readOptions {
        |      r_option_1 = "r_value_1"
        |      r_option_2 = 42
        |    }
        |    writeOptions {
        |      w_option_1 = "w_value_1"
        |      w_option_2 = false
        |    }
        |  }
        |}
      """.stripMargin

    val config = ConfigFactory.parseString(configStr)

    When("parsing this configuration")
    val sourceConfigs: Map[String, SourceConfig] = SourceManager.parseSourceConfigs(config)

    Then("sources config should be indexed and typed")
    sourceConfigs("source1") should be(SourceConfig(
      path = "source1_path",
      format = FileFormat.ORC,
      delimiter = None,
      header = None,
      partitions = Vector(),
      table = None,
      saveMode = None,
      createExternalTable = false,
      readOptions = Map(),
      writeOptions = Map()
    ))
    sourceConfigs("source2") should be(SourceConfig(
      path = "source2_path",
      format = FileFormat.Json,
      delimiter = Some(";"),
      header = Some(true),
      partitions = Vector("part1", "part2"),
      table = Some("some_table"),
      saveMode = Some(SaveMode.Overwrite),
      createExternalTable = true,
      readOptions = Map(
        "r_option_1" -> "r_value_1",
        "r_option_2" -> "42"
      ),
      writeOptions = Map(
        "w_option_1" -> "w_value_1",
        "w_option_2" -> "false"
      )
    ))
  }

}
