package com.gilcu2.utils

import org.apache.spark.sql.DataFrame

object DataFrameSameSchemeComparator {

  def areEqual(df1:DataFrame,df2:DataFrame,keyFields:Array[String]):Boolean={
    df1.cache()
    df2.cache()
    val joined = df1.join(df2, keyFields)
    val df1MinusDf2 = df1.except(df2)
    val df2MinusDf1 = df2.except(df1)
    df1MinusDf2.isEmpty && df1MinusDf2.isEmpty
  }

}
