package core

import org.apache.spark.sql.SparkSession

class SparkApplication extends App{
  lazy protected val spark = SparkSession.builder()
    .appName("Spark Application - Sandbox")
    .master("local[*]")
    .getOrCreate()

  lazy protected val sc = spark.sparkContext
}
