package it.trenzalore.utils.spark

import com.typesafe.config.ConfigFactory
import it.trenzalore.utils.spark.io.SourceConfig
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
        |  }
        |}
      """.stripMargin

    val config = ConfigFactory.parseString(configStr)

    When("parsing this configuration")
    val sourceConfigs: Map[String, SourceConfig] = SourceManager.parseSourceConfigs(config)

    Then("sources config should be indexed and typed")
    sourceConfigs("source1") should be(SourceConfig(path = "source1_path", format = FileFormat.ORC))
    sourceConfigs("source2") should be(SourceConfig(path = "source2_path", format = FileFormat.Json))
  }

}
