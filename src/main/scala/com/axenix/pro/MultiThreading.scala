package com.axenix.pro

import org.apache.spark.sql.SparkSession

import java.util.concurrent.Executors
import scala.concurrent._

object MultiThreading {

  def main(args: Array[String]): Unit = {
    val props = ArgProperties(args)
    val spark = SparkSession.builder().getOrCreate()
    val common = new Common(spark)
    // определяем пул потоков, в данном случае размерностью 2
    val numThreads = 2
    implicit val ec: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(numThreads))
    // формируем коллекцию Future объектов инкапсулирующих в себе вычислительные процессы
    val fSequence = Future.sequence(
      Future {
        // выполнение Process Genres
        ("genresDF", common.processGenres(props.moviesPath, props.genresPath))
      } ::
        Future {
          // выполнение Process Ratings
          ("avgRatingsDF", common.processRatings(props.ratingsPath, props.avgRatingsPath))
        } :: Nil
    )
    // ожидаем получение результатов из асинхронных операций
    val results = Await.result(fSequence, duration.Duration.Inf).toMap
    // получаем наборы данных по ключу
    val genresDF = results("genresDF")
    val avgRatingsDF = results("avgRatingsDF")
    // выполнение Process Statistics
    common.processStatistics(genresDF, avgRatingsDF, props.statisticsPath)
  }

}
