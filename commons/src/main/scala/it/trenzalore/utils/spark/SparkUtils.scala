package it.trenzalore.utils.spark

import org.apache.spark.sql.{ Column, DataFrame, Dataset, Encoders, SparkSession }
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.col

import scala.reflect.runtime.universe.TypeTag

object SparkUtils {

  object implicits {
    implicit class StructTypeEnhanced(st: StructType) {

      lazy val columns: Seq[Column] = st.fieldNames.map(col)

    }

    implicit class DataframeEnhanced(df: DataFrame) {

      def persistIf(condition: Boolean, storageLevel: StorageLevel): DataFrame = {
        if (condition)
          df.persist(storageLevel)
        else
          df
      }

      def to[T <: Product: TypeTag]: Dataset[T] = {
        import df.sparkSession.implicits._
        df.select(columnsOf[T]: _*).as[T]
      }

    }

    implicit class DatasetEnhanced[T](ds: Dataset[T]) {

      def persistIf(condition: Boolean, storageLevel: StorageLevel): Dataset[T] = {
        if (condition)
          ds.persist(storageLevel)
        else
          ds
      }

    }
  }

  def columnsOf[T <: Product: TypeTag]: Seq[Column] = {
    Encoders.product[T].schema.fieldNames.map(col)
  }

}
