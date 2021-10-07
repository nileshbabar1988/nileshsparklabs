package com.babarnil.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object AggregatePerformance extends App with Utils {

  import spark.implicits._

  val indf = spark.range(1,1000)
    .toDF("trn_residual_maturity").withColumn("id",lit(1))
    .withColumn("cust_id",col("trn_residual_maturity") % 10)
    .union(
      spark.range(1,1000)
        .toDF("trn_residual_maturity").withColumn("id",lit(2))
        .withColumn("cust_id",col("trn_residual_maturity") % 20)
    )
    .union(
      spark.range(1,1000)
        .toDF("trn_residual_maturity").withColumn("id",lit(3))
        .withColumn("cust_id",col("trn_residual_maturity") % 100)
    )


  def aggregateUsingWindow(df : DataFrame) : DataFrame = {
    val window = Window.partitionBy(col("cust_id"))
    val resDf = df.withColumn("aggBal", sum(col("trn_residual_maturity")) over window)
    println(resDf.count())
    resDf
  }

  def aggregateSelfJoin(df : DataFrame) : DataFrame = {
    val aggDf = df.groupBy(col("cust_id")).agg(sum("trn_residual_maturity")).as("aggBal")
    val resDf = df.join(aggDf, Seq("cust_id"))
    println(resDf.count())
    resDf
  }

  println(" Started Aggregate Using Self Join ")
  benchmark("aggregate_Self_Join")(aggregateSelfJoin(indf))
  println(" Completed Aggregate Using Self Join ")


  println(" Started Aggregate Using Window ")
  benchmark("aggregate_Window")(aggregateUsingWindow(indf))
  println(" Completed Aggregate Using Window ")
}
