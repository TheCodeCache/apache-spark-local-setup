package org.apache.spark.streaming.test

import scala.collection.mutable.Queue

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds

/**
 * Streaming Code - To test Local set up of Apache-Spark
 *
 * @author manoranjan
 */
object TestStreamSetup extends App {

  println("Basic Streaming Demonstration!")

  val ssc = SparkUtil.getSparkStreamingContext("Queue-Streaming", Seconds(5))

  val rdd1 = ssc.sparkContext.parallelize(Array(1, 2, 3))
  val rdd2 = ssc.sparkContext.parallelize(Array(4, 5, 6))
  val rdd3 = ssc.sparkContext.parallelize(Array(7, 8, 9))

  val rddQueue = new Queue[RDD[Int]]

  rddQueue.enqueue(rdd1)
  rddQueue.enqueue(rdd2)
  rddQueue.enqueue(rdd3)

  val numberDStream = ssc.queueStream(rddQueue, true)
  val plusOneDStream = numberDStream.map(Predef.identity)

  plusOneDStream.print()

  ssc.start()
  ssc.awaitTerminationOrTimeout(8000)
}