name := "positionlIndexing"

version := "0.1"

scalaVersion := "2.12.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.5.3"

libraryDependencies ++= Seq("com.github.melrief" %% "purecsv" % "0.1.1")

libraryDependencies += "org.elasticsearch" % "elasticsearch-hadoop" % "5.1.1"
