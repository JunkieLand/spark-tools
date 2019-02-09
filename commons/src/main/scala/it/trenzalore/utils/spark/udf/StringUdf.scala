package it.trenzalore.utils.spark.udf

import org.apache.spark.sql.functions.udf

object StringUdf {

  def splitEveryNChar(n: Int) = udf((s: String) ⇒ s.grouped(n).toVector)

}
