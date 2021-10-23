package com.babarnil.spark

import org.scalatest.{Matchers, Assertion}
import org.apache.spark.sql.DataFrame

object DataFrameUtils extends Matchers with Utils {

  import spark.implicits._

  def checkDataframeEquality(actualDf : DataFrame, expectedDf : DataFrame,
                             columnNametoCheckAgainst : String, equalityCheck : Boolean = true ) : Assertion =
  {

  }

}
