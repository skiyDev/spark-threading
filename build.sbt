import Dependencies._

ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "spark-threading"
  )

libraryDependencies ++= Seq(
  sparkSql, sparkTestSql, scalaTest
)