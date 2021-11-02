package org.apache.spark.sql.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
//import org.apache.spark.streaming.Seconds
//import org.apache.spark.streaming.StreamingContext
import scala.collection.mutable.Queue
import org.apache.spark.rdd.RDD

object SparkUtil {

  System.setProperty("hadoop.home.dir", "E:\\winutils-master\\hadoop-3.3.0")

  def getSparkConfig(appName: Class[_], thread: String) = {
    val conf = new SparkConf().setAppName(appName.getSimpleName)
      .setMaster("local[" + thread + "]");
    conf.set("spark.sql.orc.enabled", "true")
  }

  def getSparkSession(appName: Any, thread: String = "4") =
    SparkSession.builder().config(getSparkConfig(appName.getClass(), thread))
      .appName(this.getClass.getSimpleName).getOrCreate()
  
  def getSparkContext(appName: Any, thread: String = "4") = getSparkSession(appName, thread)
      
//  def getSparkStreamingContext(appName: Any, interval: Seconds, thread: String = "4") = 
//    new StreamingContext(getSparkConfig(appName, thread), interval: Seconds)
}