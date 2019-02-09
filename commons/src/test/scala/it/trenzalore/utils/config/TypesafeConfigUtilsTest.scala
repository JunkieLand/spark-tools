package it.trenzalore.utils.config

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }
import scala.collection.JavaConversions._
import TypesafeConfigUtils._

class TypesafeConfigUtilsTest extends FlatSpec with Matchers with GivenWhenThen {

  it should "parse all inner config to a Map, and replace all config ending with '.value'" in {
    Given("a configuration with properties ending with '.value'")
    val config = ConfigFactory.parseMap(Map(
      "wrapper.inner.value" -> "no_more_value",
      "wrapper.inner.name" -> "toto",
      "wrapper.inner.age" -> "42"
    ))

    When("getting all inner config to a Map")
    val configAsMap = config.getConfig("wrapper").flattenToMap

    Then("values should be there without '.value'")
    configAsMap("inner") should be("no_more_value")
    configAsMap("inner.name") should be("toto")
    configAsMap("inner.age") should be("42")
  }

}
