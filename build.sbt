name := "nileshsparklabs"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % sparkVersion,

  "org.apache.spark" %% "spark-streaming" % sparkVersion,

  "org.apache.spark" %% "spark-sql" % sparkVersion,

  "org.apache.spark" %% "spark-launcher" % sparkVersion,

  "org.apache.spark" %% "spark-catalyst" % sparkVersion,

  "org.apache.spark" %% "spark-streaming" % sparkVersion,

  "org.scalatest" %% "scalatest" % "3.0.8" % Test

)