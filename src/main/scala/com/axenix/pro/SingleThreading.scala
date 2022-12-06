package com.axenix.pro

import org.apache.spark.sql.SparkSession

object SingleThreading {
  def main(args: Array[String]): Unit = {
    val props = ArgProperties(args)
    val spark = SparkSession.builder().getOrCreate()
    val common = new Common(spark)
    // выполнение Process Genres
    val genresDF = common.processGenres(props.moviesPath, props.genresPath)
    // выполнение Process Ratings
    val avgRatingsDF = common.processRatings(props.ratingsPath, props.avgRatingsPath)
    // выполнение Process Statistics
    common.processStatistics(genresDF, avgRatingsDF, props.statisticsPath)
  }
}
