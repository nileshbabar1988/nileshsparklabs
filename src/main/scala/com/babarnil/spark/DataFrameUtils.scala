package com.babarnil.spark

import org.scalatest.{Assertion, Assertions, Matchers}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

import scala.util.Try

object DataFrameUtils extends Matchers with Utils {

  import spark.implicits._
  val emptyDataFrame : DataFrame = Seq(1).toDF("dummyColumn")

  def checkDataframeEquality(actualDf : DataFrame, expectedDf : DataFrame,
                             columnNametoCheckAgainst : String,
                             equalityCheck : Boolean = true ) : Assertion =
  {
    schemaEquals(actualDf.schema, expectedDf.schema)
    if(equalityCheck)
      actualDf.select(columnNametoCheckAgainst).collect() should equal (expectedDf.select(columnNametoCheckAgainst).collect())
    else
      actualDf.select(columnNametoCheckAgainst).collect() should contain noElementsOf (expectedDf.select(columnNametoCheckAgainst).collect())
  }


  def schemaEquals( structType1 : StructType, structType2 : StructType) : Boolean = {
    if(structType1.length != structType2.length)
      false
    else
      {
        structType1.zip(structType2).forall{
          t => (t._1.name == t._2.name ) && (t._1.dataType == t._2.dataType)
        }
      }
  }


  def replicateRowMoreTimes(n:Int)(df: DataFrame) : DataFrame =
    df
    .sqlContext
    .createDataFrame(
      df.sparkSession.sparkContext.parallelize(
        List.fill(n)(df.take(1)(0))
      ),
      df.schema
    )

  def dataGenerator(data : Map[String, List[String]], initialDf : DataFrame) : DataFrame = {
    val size : Int = Try{
      data.mapValues(_.size).maxBy(_._2)._2
    }.getOrElse(0)
    val dataDf : DataFrame = if(initialDf.count > 0 ) initialDf else emptyDataFrame
    val replicate : DataFrame = replicateRowMoreTimes(size)(dataDf)
      .repartition(1)
      .withColumn("id",monotonically_increasing_id())
    generateDfFromNormalisedList(replicate,normaliseList(data,size))
  }

  def normaliseList(columnWithValues: Map[String, List[String]], size : Int) : Map[String, List[String]] =
    columnWithValues
      .mapValues( ls =>
      ls ::: List.fill(size - ls.size)(ls.lastOption.getOrElse("-1")))

  def generateDfFromNormalisedList(df : DataFrame, columnWithValues: Map[String, List[String]]) : DataFrame =
    columnWithValues
    .foldLeft(df)((accDf, columnWithData) => {
      val df = columnWithData._2.toDF(columnWithData._1)
        .withColumn("id", monotonically_increasing_id())
      accDf
        .join(df, Seq("id"))
        .drop(df("id"))
    })

  def generateDataAndTest(inputDf : DataFrame, f: DataFrame => Assertion,
                          data : Map[String, List[Any]] = Map.empty[String, List[Any]]) : Assertion = {
    val dataDf : DataFrame = if(data.nonEmpty) dataGenerator(
      data
        .mapValues{
          ls =>
            ls.map{ i =>
              if(i == null)
                ""
              else i.toString
            }
        }, inputDf) else inputDf
    assert(dataDf.count() > 0 , "DataGeneration returned no Data")
    f(dataDf)
  }

  def testDataChecker(data: (String, List[Any]), input : DataFrame, f: DataFrame => Assertion) : List[Assertion] = {
    data._2.map{
      i =>
        val inputDf = input.withColumn(data._1, lit(1))
        f(inputDf)
    }
  }

  def testDataChecker(input : DataFrame, f: DataFrame => Assertion) =
    f(input)


  def checkFlag[A](columnName : String,
                   expectedColumnValue : A,
                   equalityCheck : Boolean,
                   transformation : DataFrame => DataFrame)
                  (df : DataFrame) : Assertion = {
    val actualRsult = df.transform(transformation)
    val expectedResult = df.withColumn(columnName , lit(expectedColumnValue))
    checkDataframeEquality(actualRsult, expectedResult, columnName, equalityCheck)
  }

  def getTestResult(resultType : DataFrame, resultColumnId : Int,
                    colName : String,
                    getResultFromColumn : String = "f_position_balance_id") : String =
    resultType
    .filter(resultType(getResultFromColumn) === lit(resultColumnId))
    .collect()
    .headOption
    .map( row => {
    Try(row.getAs[String](colName))
    .getOrElse("No column found with Name " + colName)
    }).getOrElse(s"No Data found for $getResultFromColumn : $resultColumnId")

  def getTestIntResult(resultType : DataFrame, resultColumnId : Int,
                    colName : String,
                    getResultFromColumn : String = "f_position_balance_id"): Any =
    resultType
      .filter(resultType(getResultFromColumn) === lit(resultColumnId))
      .collect()
      .headOption
      .map( row => {
        Try(row.getAs[Any](colName))
          .getOrElse("No column found with Name " + colName)
      }).getOrElse(s"No Data found for $getResultFromColumn : $resultColumnId")


  def createDfWithColumn(input: DataFrame, columnList : List[String], defaultValue : String = "UNKNOWN") : DataFrame = {
    columnList.foldLeft(input)((input, columnName) => input.withColumn(columnName, lit(defaultValue)))
  }
}
