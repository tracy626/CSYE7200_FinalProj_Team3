name := """SparkApp"""

version := "1.0"

scalaVersion := "2.11.8"

scalacOptions in(Compile, doc) ++= Seq("-groups", "-implicits", "-deprecation", "-Ywarn-dead-code", "-Ywarn-value-discard", "-Ywarn-unused" )

parallelExecution in Test := false

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.3" % "test",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.0.0",
  "org.mongodb" % "casbah_2.11" % "3.1.1",
  "org.apache.spark" %% "spark-core" % "2.1.1",
  "org.apache.spark" %% "spark-sql" % "2.1.1",
  "org.apache.spark" %% "spark-mllib" % "2.1.1"
)

parallelExecution in Test := false