package org.apache.spark.sql.test

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

/**
 * Test-Codes to verify local set up of Apache-Spark
 *
 * @author manoranjan
 */
object TestMe extends App {

  println("Test Me!")

  val spark = SparkUtil.getSparkSession("First Spark Test!")

  val rdd = spark.sparkContext.parallelize(Seq(1, 2, 3, 4, 5), 2)

  rdd.map(x => x * 2).filter(y => y % 2 == 0).foreach(println)

  println
  println("I've been tested!")

  import spark.implicits._

  val df = Seq(("hi", "1"), ("hello", "2")).toDF("column-1", "column-2")

  df.printSchema
  df.show(false)

  println
  println("Testing Ends!")
}