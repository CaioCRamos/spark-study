package part1dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App{
  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Counting
  moviesDF.select(count(col("Major_Genre"))).show() // all the values except null
  moviesDF.selectExpr("count(Major_Genre)").show() // same thing

  // Counting all
  moviesDF.select(count("*")).show() // count all the rows, include nulls

  // Counting distinct
  moviesDF.select(countDistinct(col("Major_Genre"))).show()

  // Approximate counting
  moviesDF.select(approx_count_distinct(col("Major_Genre"))).show()

  // Min and Max
  moviesDF.select(min(col("IMDB_Rating"))).show()
  moviesDF.selectExpr("min(IMDB_Rating)").show()

  // Sum
  moviesDF.select(sum(col("US_Gross"))).show()
  moviesDF.selectExpr("sum(US_Gross)").show()

  // Average
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating"))).show()
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)").show()

  // Data Science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  ).show()

  // Grouping
  moviesDF
    .groupBy(col("Major_Genre")) // includes null
    .count()
    .show()

  moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")
    .show()

  moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))
    .show()

  // Exercises

  // 1. sum up all the profits of all the movies
  moviesDF
    .select(sum(col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")))
    .show()

  // 2. Count how many distinct directors
  moviesDF
    .select(countDistinct(col("Director")))
    .show()

  // 3. Show the mean and standard deviation for US Gross
  moviesDF.select(
    mean("US_Gross"),
    stddev("US_Gross")
  ).show()

  // 4.
  moviesDF
    .groupBy(col("Director"))
    .agg(
      avg("IMDB_Rating").as("Avg_Rating"),
      sum("US_Gross").as("Total_US_Gross")
    )
    .orderBy(col("Avg_Rating").desc_nulls_last)
    .show()
}
