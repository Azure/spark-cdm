name := "spark-cdm"

version := "0.2"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2" % "provided"
libraryDependencies += "com.microsoft.azure" % "adal4j" % "1.6.3"
libraryDependencies += "com.univocity" % "univocity-parsers" % "2.7.6"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
