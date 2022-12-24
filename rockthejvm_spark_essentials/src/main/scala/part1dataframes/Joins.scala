package part1dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object Joins extends App {
  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val bandsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/bands.json")
  val guitarPlayersDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitarPlayers.json")
  val guitarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitars.json")

  val joinCondition = guitarPlayersDF.col("band") === bandsDF.col("id")

  // Inner join
  guitarPlayersDF
    .join(bandsDF, joinCondition, "inner")
    .show()

  // Outer join
  // left outer = everything in the inner join + all the rows in the LEFT table, with nulls in where the data is missing
  guitarPlayersDF
    .join(bandsDF, joinCondition, "left_outer")
    .show()

  // right outer = everything in the inner join + all the rows in the RIGHT table, with nulls in where the data is missing
  guitarPlayersDF
    .join(bandsDF, joinCondition, "right_outer")
    .show()

  // full outer join = everything in the inner join + all the rows in the BOTH table, with nulls in where the data is missing
  guitarPlayersDF
    .join(bandsDF, joinCondition, "outer")
    .show()

  // Semi-joins = everything in the inner join, but only rows from the LEFT table
  guitarPlayersDF
    .join(bandsDF, joinCondition, "left_semi")
    .show()

  // Anti-joins = everything from LEFT table that don't have a match in the RIGHT table
  guitarPlayersDF
    .join(bandsDF, joinCondition, "left_anti")
    .show()

  // Things to bare in mind
  // Repeated column after JOIN, like ID

  // option 1 - rename the column on with we are joining
  guitarPlayersDF
    .join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2 - drop the dupe column
  guitarPlayersDF
    .join(bandsDF, joinCondition, "inner")
    .drop(bandsDF.col("id"))

  // option 3 - rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitarPlayersDF
    .join(bandsModDF, guitarPlayersDF.col("band") === bandsModDF.col("bandId"), "inner")

  // using complex types
  guitarPlayersDF
    .join(guitarsDF.withColumnRenamed("id", "guitarId"),
      expr("array_contains(guitars, guitarId"))
}
