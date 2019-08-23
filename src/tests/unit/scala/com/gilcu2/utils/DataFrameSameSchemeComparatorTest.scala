package com.gilcu2.utils

import com.gilcu2.testUtil.SparkSessionTestWrapper
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import DataFrameSameSchemeComparator._

class DataFrameSameSchemeComparatorTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "DataFrameSameSchemeComparator"

  it should "return false when the dataframes have different rows" in {

    Given("two dataframes with the same scheme and different rows")

    val field1 = "id"
    val field2 = "animal"

    val schema = List(
      StructField(field1, IntegerType, true),
      StructField(field2, StringType, true)
    )

    val data1 = Seq(
      Row(8, "bat"),
      Row(64, "mouse"),
      Row(-27, "horse")
    )
    val data2 = Seq(
      Row(8, "batman"),
      Row(64, "mouse"),
      Row(-27, "horse")
    )

    val df1 = spark.createDataFrame(
      spark.sparkContext.parallelize(data1),
      StructType(schema)
    )
    val df2 = spark.createDataFrame(
      spark.sparkContext.parallelize(data2),
      StructType(schema)
    )

    When("the dataframes are compared")
    val result = areEqual(df1, df2, Array(field1))

    Then("the reult must be false")
    result shouldBe false
  }

  it should "return true when the dataframes have equal rows and whatever order" in {

    Given("two different dataframes with the same scheme")

    val field1 = "id"
    val field2 = "animal"

    val schema = List(
      StructField(field1, IntegerType, true),
      StructField(field2, StringType, true)
    )

    val data1 = Seq(
      Row(8, "bat"),
      Row(64, "mouse"),
      Row(-27, "horse")
    )
    val data2 = Seq(
      Row(64, "mouse"),
      Row(8, "bat"),
      Row(-27, "horse")
    )

    val df1 = spark.createDataFrame(
      spark.sparkContext.parallelize(data1),
      StructType(schema)
    )
    val df2 = spark.createDataFrame(
      spark.sparkContext.parallelize(data2),
      StructType(schema)
    )

    When("the dataframes are compared")
    val result = areEqual(df1, df2, Array(field1))

    Then("the reult must be true")
    result shouldBe true
  }

}

