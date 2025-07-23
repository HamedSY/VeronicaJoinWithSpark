scalaVersion := "3.7.1"

name := "VeronicaJoinWithSpark"
version := "1.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "4.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.0.0" % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19"