package org.apache.spark.streaming.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext

/**
 * utility for spark streaming context
 */
object SparkUtil {

  System.setProperty("hadoop.home.dir", "E:\\winutils-master\\hadoop-3.3.0")

  def getSparkConfig(appName: Class[_], thread: String) = {
    val conf = new SparkConf().setAppName(appName.getSimpleName)
      .setMaster("local[" + thread + "]");
    //conf.set("spark.sql.orc.enabled", "true")
    conf
  }

  def getSparkContext(appName: Any, thread: String = "4") =
    new SparkContext(getSparkConfig(appName.getClass, thread))

  def getSparkStreamingContext(appName: Any, interval: Duration, thread: String = "4") =
    new StreamingContext(getSparkConfig(appName.getClass, thread), interval: Duration)
}