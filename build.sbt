scalaVersion := "2.13.16"

name := "VeronicaJoinWithSpark"
version := "1.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.1"

fork := true

javaOptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"

classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
