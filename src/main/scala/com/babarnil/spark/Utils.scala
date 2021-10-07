package com.babarnil.spark

import java.io.ByteArrayOutputStream

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StructType}

trait Utils {


  val spark  = SparkSession
    .builder()
    .appName("MySparkApp")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  spark.conf.set("spark.sql.crossJoin.enabled",true)
  spark.conf.set("spark-sql-shuffle-partitions",40)

  import spark.implicits._

  def read (path : String, format : String = "csv", header : String = "true",
            dateFormat : String = "yyyy-mm-dd", inferSchema : String = "true") : DataFrame =
  {
    spark.read.format(format)
      .option("inferschema", inferSchema)
      .option("header",header)
      .option("dateFormat",dateFormat)
      .load(path)
  }

  def benchmark ( name : String, times : Int = 10, warmups : Int = 6) (f: => Unit) = {
    for(i <- 1 to warmups){f}
    println("warm up done")
    val startTime = System.nanoTime()
    for(i <- 1 to times)
      {
        println(s"Running for $i Iteration out of $times")
        f
      }
    val endTime = System.nanoTime()
    val finalRes = (endTime - startTime).toDouble / (times * 1000000.0)
    println(s"Average Time Taken in $name for $times ruuns : $finalRes millis")
    finalRes
  }


  def logDfPartitionsStats (df: DataFrame): Unit = {
    val partsSeq = df.withColumn("partition_id", spark_partition_id())
      .groupBy("partition_id").count()
    val baos = new ByteArrayOutputStream()
    val col = partsSeq.columns(1)
    val minMaxVal = partsSeq.agg(min(col),max(col))
    Console.withOut(baos){
      minMaxVal.show()
      partsSeq.orderBy(asc("count")).show(10)
      partsSeq.orderBy(desc("count")).show(10)
    }
    println(s" ( count ${partsSeq.count()} ) \n ${baos.toString()}")
  }

  final def jsonToCsvDFConverter (df : DataFrame ) : DataFrame = {
    if(df.schema.fields.find(_.dataType match {
      case ArrayType(StructType(_),_) | StructType(_) => true
      case _ => false
    }).isEmpty) df
    else {
      val columns = df.schema.fields.map( f => f.dataType match {
        case _ : ArrayType => explode(col(f.name.toLowerCase())).as(f.name.toLowerCase())
        case s : StructType => col(s"${f.name.toLowerCase}.*")
        case _ => col(f.name.toLowerCase())
      })
      jsonToCsvDFConverter(df.select(columns:_*))
    }
  }

  def convertDfToLowercase (df : DataFrame) : DataFrame = {
    val columns = df.schema.fields.map( f => f.name match {
      case _ => col(f.name).as(f.name.toLowerCase())
    })
    df.select(columns:_*)
  }

}
