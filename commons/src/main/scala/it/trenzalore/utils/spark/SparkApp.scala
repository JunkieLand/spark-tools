package it.trenzalore.utils.spark

import java.net.URI

import it.trenzalore.utils.config.Configurable
import it.trenzalore.utils.logging.Logging
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import it.trenzalore.utils.config.TypesafeConfigUtils._

trait SparkApp { self: Logging with Configurable ⇒

  lazy val appName = config.getString("app.name")

  lazy val sparkSettings = {
    if (config.hasPath("spark")) {
      logger.info(s"SparkSession will use settings ${config.getConfig("spark")}")
      config.getConfig("spark").flattenToMap.map { case (k, v) ⇒ (k.toString, v.toString) }
    } else {
      Map.empty[String, String]
    }
  }

  implicit lazy val spark = {
    val sparkConf = new SparkConf().setAll(sparkSettings)
    val _spark = SparkSession.builder().appName(appName).config(sparkConf).getOrCreate()
    _spark.sparkContext.setLogLevel("WARN")
    _spark
  }

  implicit lazy val fs = FileSystem.get(new URI(config.getString("datalake.root")), spark.sparkContext.hadoopConfiguration)

}
