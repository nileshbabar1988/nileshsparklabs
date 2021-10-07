package com.babarnil.spark

import org.apache.spark.sql.SparkSession

object SparkHelloWorld extends App with Utils {


  val sc = SparkSession.builder()
    .appName("myApp")
    .master("local")
    .getOrCreate()


  import sc.implicits._

  val df = Seq((1,2), (1,2)).toDF()

  df.show()

  logDfPartitionsStats(df)


  Thread.sleep(500000)
}
