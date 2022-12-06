package com.axenix.pro

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.Executors
import scala.concurrent._

class SparkTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder.master("local").getOrCreate()
  val props: ArgProperties = ArgProperties(
    moviesPath = ".\\data\\movies.csv",
    genresPath = ".\\data\\result\\genres",
    ratingsPath = ".\\data\\ratings.csv",
    avgRatingsPath = ".\\data\\result\\avg_ratings",
    statisticsPath = ".\\data\\result\\statistics"
  )
  val common = new Common(spark)
  val expectedRowCount = 9725

  def getCurrentRowCount: Long = spark.read.csv(props.statisticsPath).count

  test("Single-threading unit test-case") {
    val genresDF = common.processGenres(props.moviesPath, props.genresPath)
    // выполнение Process Ratings
    val avgRatingsDF = common.processRatings(props.ratingsPath, props.avgRatingsPath)
    // выполнение Process Statistics
    common.processStatistics(genresDF, avgRatingsDF, props.statisticsPath)

    assertResult(expectedRowCount)(getCurrentRowCount)
  }

  test("Multi-threading unit test-case") {
    val numThreads = 2
    implicit val ec: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(numThreads))

    val fSequence = Future.sequence(
      Future {
        ("genresDF", common.processGenres(props.moviesPath, props.genresPath))
      } ::
        Future {
          ("avgRatingsDF", common.processRatings(props.ratingsPath, props.avgRatingsPath))
        } :: Nil
    )

    val results = Await.result(fSequence, duration.Duration.Inf).toMap

    val genresDF = results("genresDF")
    val avgRatingsDF = results("avgRatingsDF")

    common.processStatistics(genresDF, avgRatingsDF, props.statisticsPath)

    assertResult(expectedRowCount)(getCurrentRowCount)
  }

}
