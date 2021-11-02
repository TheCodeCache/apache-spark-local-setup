package org.apache.spark.sql.test

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

/**
 * Batch Code - To test Local set up of Apache-Spark
 *
 * @author manoranjan
 */
object TestBatchSetup extends App {

  val spark = SparkUtil.getSparkSession("Batch Processing - Spark Setup Test!")

  // should throw exception, as we do not have shell script exec environment on Windows,
  // but this would work on linux env as the program "ps -e" will run there
  var input: java.io.BufferedReader = null
  try {
    val p = Runtime.getRuntime().exec("ps -e")
    val input = new java.io.BufferedReader(new java.io.InputStreamReader(p.getInputStream()))
    var line = ""
    while (line != null) {
      line = input.readLine()
      println(line) // parse data here.
    }
  } catch {
    case ex: Exception => println(ex)
  } finally {
    if (input != null) input.close()
  }

  //circular object creation - does not make sense
  try {
    class CircularObject() {
      val circularObject = new CircularObject()

      def main(args: Array[String]): Unit = {
        println("Test - creating circular object")
      }
    }
  } catch {
    case err: Error => println(err)
  }

  // spark rdd tests
  val count = spark.sparkContext.parallelize(1 to 10).filter {
    _ =>
      val x = math.random
      val y = math.random
      x * x + y * y < 1
  }.count()

  println(s"\nPI is roughly ${4.0 * count / 10}\n")

  // spark dataframe tests
  println("spark batch-processing test begins")

  val options = scala.collection.mutable.Map[String, String]()

  options("inferSchema") = "false"
  options("delimiter") = ","
  options("mode") = "PERMISSIVE"
  options("header") = "true"

  // this option is crucial for letting spark capture bad records
  val CORRUPT_RECORDS_COLUMN = "_corrupt_records"
  options("columnNameOfCorruptRecord") = CORRUPT_RECORDS_COLUMN

  // to trime all the leading and trailing spaces
  options("ignoreLeadingWhiteSpace") = "true"
  options("ignoreTrailingWhiteSpace") = "true"

  // To enable escape
  options("escape") = "\\"
  options("quoteEscape") = "\""
  options("charToEscapeQuoteEscaping") = "\\"

  // that means the file containing new line character would not be parsed as good record
  // due to performance readons in spark
  // options("multiLine") = "true"

  val schema = StructType(
    List(
      StructField("account", StringType, true),
      StructField("details", StringType, true),
      StructField("description", StringType, true)))

  val newSchema = schema.add(CORRUPT_RECORDS_COLUMN, StringType, true)

  println("Loading csv file into spark DF:")
  // loads the delimited file and creates a dataframe
  val csvDF = spark.read.format("csv").options(options).schema(newSchema).load("E:\\testdata\\test_double_quote.csv")

  csvDF.printSchema()
  csvDF.show(false)

  println("csv parsing completes")
}