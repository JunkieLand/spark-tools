package com.renault.hercules.traceability

import java.io.{ PrintWriter, StringWriter }

import org.apache.spark.sql._
import org.scalatest._

/**
  * Base class for testing Spark DataFrames.
  */
trait DataFrameSuiteBase extends Suite
  with SparkSessionWrapper
  with BeforeAndAfterAll {

  //  if (enableHiveSupport)
  //    EmbeddedDerby.start()

  final def stackTraceToString(e: Throwable): String = {
    val sw = new StringWriter
    val pw = new PrintWriter(sw)
    try {
      e.printStackTrace(pw)
      sw.toString
    } finally {
      pw.close()
      sw.close()
    }
  }

  override protected def afterAll(): Unit = {
    spark.sqlContext.clearCache()
    spark.sparkContext.clearJobGroup()
    spark.close()
    stopMetastore()
    super.afterAll()
  }

  def stopMetastore() {
    //    EmbeddedDerby.stop()
  }
}

trait SparkSessionWrapper {
  lazy val enableHiveSupport: Boolean = false
  implicit lazy val spark: SparkSession = {
    val builder = SparkSession.builder()
      .master("local[*]")
      .appName("test suite")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", 16)
      .config("spark.default.parallelism", 16)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.crossJoin.enabled", "true")

    if (enableHiveSupport)
      builder.enableHiveSupport()
    builder.getOrCreate()
  }
}