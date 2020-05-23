name := "deltalake-demo"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"
libraryDependencies += "io.delta" %% "delta-core" % "0.6.0"
libraryDependencies += "log4j" % "log4j" % "1.2.17"

//configuration file dependency
libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "com.typesafe" % "config" % "1.2.1"
)