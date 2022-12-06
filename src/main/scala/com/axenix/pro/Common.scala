package com.axenix.pro

import org.apache.spark.sql.functions.{avg, count, explode, first, max, min, split}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

class Common(spark: SparkSession) {

  import spark.implicits._

  def processGenres(moviesPath: String, genresPath: String): DataFrame = {
    // определение схемы данных для фильмов
    val genreDFSchema = StructType(
      StructField("movieId", IntegerType, true) ::
        StructField("title", StringType, true) ::
        StructField("genres", StringType, true) :: Nil
    )

    val movieDF: DataFrame = spark.read
      .option("header", true)
      .schema(genreDFSchema)
      .csv(moviesPath)
    // формируем набор данных где в каждой строке id фильму соответствует его жанр
    val genreDF: DataFrame = movieDF
      .withColumn("genres", split($"genres", "\\|"))
      .select($"movieId",explode($"genres").as("genre"))

    genreDF.write.mode("overwrite")
      .option("header", "true").csv(genresPath)

    val uploadedGenresDF = spark.read.option("header", true)
      .schema(
        StructType(
          StructField("movieId", IntegerType, true) ::
            StructField("genre", StringType, true) :: Nil
        )
      ).csv(genresPath)

    uploadedGenresDF
  }

  def processRatings(ratingsPath: String, calcRatingsPath: String): DataFrame = {
    // определение схемы данных для пользовательских оценок
    val ratingDFSchema = StructType(
      StructField("userId", IntegerType, true) ::
        StructField("movieId", IntegerType, true) ::
        StructField("rating", FloatType, true) ::
        StructField("timestamp", IntegerType, true) :: Nil
    )

    val ratingDF = spark.read.option("header", true)
      .schema(ratingDFSchema)
      .csv(ratingsPath)
      .select("movieId", "rating")
    // формируем набор данных где id фильму соответствует его средняя пользовательская оценка
    val avgRatingDF = ratingDF.groupBy("movieId")
      .agg(avg("rating").alias("avg_movie_rating"))
      .orderBy("movieId")

    avgRatingDF.write.mode("overwrite")
      .option("header", "true").csv(calcRatingsPath)

    val uploadedAvgRatingDF =
      spark.read.option("header", true)
        .schema(
          StructType(
            StructField("movieId", IntegerType, true) ::
            StructField("avg_movie_rating", FloatType, true) :: Nil)
        ).csv(calcRatingsPath)

    uploadedAvgRatingDF
  }

  def processStatistics(genresDF: DataFrame, avgRatingsDF: DataFrame, statisticsPath: String): Unit = {
    // выполняем объединение данных с жанрами и оценками, вычисляем количество жанров для каждого фильма
    val statisticsDF = genresDF.join(avgRatingsDF,
      genresDF("movieId") === avgRatingsDF("movieId"), "inner")
      .drop(avgRatingsDF("movieId"))
      .groupBy("movieId")
      .agg(
        count("movieId").as("genres_count"),
        first("avg_movie_rating").as("avg_movie_rating")
      )

    statisticsDF.write.mode("overwrite").option(
      "header", "true").csv(statisticsPath)
  }
}
