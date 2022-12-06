package com.axenix.pro

object ArgProperties {
  def apply(args: Array[String]) =
    new ArgProperties(
      moviesPath = args(0),
      genresPath = args(1),
      ratingsPath = args(2),
      avgRatingsPath = args(3),
      statisticsPath = args(4)
    )

}


case class ArgProperties(moviesPath: String,
                        genresPath: String,
                        ratingsPath: String,
                        avgRatingsPath: String,
                        statisticsPath: String)