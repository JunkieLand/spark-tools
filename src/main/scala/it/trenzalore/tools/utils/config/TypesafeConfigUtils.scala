package it.trenzalore.tools.utils.config

import com.typesafe.config.Config

import scala.collection.JavaConversions._

object TypesafeConfigUtils {

  implicit class EnhancedConfig(config: Config) {

    /**
      * inner {
      *   value = 1
      *   name = "toto"
      * }
      * will be transformed to :
      * Map(
      *   "inner" -> "1",
      *   "inner.name" -> "toto"
      * )
      */
    def flattenToMap: Map[String, String] = {
      config
        .entrySet()
        .map { entry ⇒
          val key = entry.getKey.replaceFirst("\\.value$", "")
          val valueAsString = entry.getValue.unwrapped().toString
          key -> valueAsString
        }
        .toMap
    }

  }

}
