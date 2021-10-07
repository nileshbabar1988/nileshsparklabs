package com.babarnil.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd

object SparkCheckpointTest extends App with  Utils {

  spark.sparkContext.setCheckpointDir("checkpointDir")

  import spark.implicits._

  val rdd  = spark.sparkContext.parallelize(1 to 100)
    .map(x => (x % 3 , 1)).reduceByKey(_ + _ )

  //rdd.checkpoint()

  val indChk = rdd.mapValues(_ > 4)

  indChk.checkpoint()
  indChk.toDebugString
  indChk.count()

  println(indChk.toDebugString)



/*  (2) MapPartitionsRDD[3] at mapValues at SparkCheckpointTest.scala:17 []
  |  ShuffledRDD[2] at reduceByKey at SparkCheckpointTest.scala:13 []
  +-(2) MapPartitionsRDD[1] at map at SparkCheckpointTest.scala:13 []
  |  ParallelCollectionRDD[0] at parallelize at SparkCheckpointTest.scala:12 []*/


/*  val rddChk  = spark.sparkContext.parallelize(1 to 100)
    .map(x => (x % 3 , 1)).reduceByKey(_ + _ )

  rddChk.checkpoint()

  val indChk1 = rdd.mapValues(_ > 4)

  println(indChk1.toDebugString)
  */

}
