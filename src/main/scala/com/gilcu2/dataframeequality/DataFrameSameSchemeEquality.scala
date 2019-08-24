package com.gilcu2.dataframeequality

import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataFrameSameSchemeEquality {
  def areEqual(df1: DataFrame, df2: DataFrame, keyFields: Array[String])(implicit spark: SparkSession): Boolean
}

object EqualityByExcept extends DataFrameSameSchemeEquality {

  def areEqual(df1: DataFrame, df2: DataFrame, keyFields: Array[String])(implicit spark: SparkSession): Boolean = {
    df1.cache()
    df2.cache()

    val count1 = df1.count()
    val count2 = df2.count()

    val df1MinusDf2 = df1.except(df2)

    count1 == count2 && df1MinusDf2.isEmpty
  }

}

object EqualityByHashcode extends DataFrameSameSchemeEquality {

  def areEqual(df1: DataFrame, df2: DataFrame, keyFields: Array[String])(implicit spark: SparkSession): Boolean = {

    df1.cache()
    df2.cache()

    val count1 = df1.count()
    val count2 = df2.count()

    val hash1 = hashCode(df1)
    val hash2 = hashCode(df2)

    count1 == count2 && hash1 == hash2
  }

  private def hashCode(df: DataFrame)(implicit spark: SparkSession): Long = {
    import spark.implicits._
    df.map(_.hashCode().toLong).reduce(_ + _)
  }

}
