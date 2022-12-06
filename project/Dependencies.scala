import sbt._

object Dependencies {
  val sparkVersion = "3.3.1"
  val scalaTestVersion = "3.2.14"

  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  val sparkTestSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "test"
  val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
}