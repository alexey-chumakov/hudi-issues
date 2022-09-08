name := "hudi-issues"

version := "0.1"

scalaVersion := "2.12.14"

idePackagePrefix := Some("hudi.issue.example")

val Spark = "3.2.1"
val Hudi = "0.11.0"
val Logging = "3.9.4"

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % Logging,
  "org.apache.hudi" %% "hudi-spark3-bundle" % Hudi,
  "org.apache.spark" %% "spark-sql" % Spark,
  "org.apache.spark" %% "spark-core" % Spark,
  "org.apache.spark" %% "spark-hive" % Spark
)

Compile / run / mainClass := Some("hudi.issue.example.aws.GlueSyncIssueExample")