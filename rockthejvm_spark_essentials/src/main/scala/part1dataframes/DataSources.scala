package part1dataframes

import org.apache.hadoop.mapreduce.jobhistory.EventWriter.WriteMode
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object DataSources extends App {
  var spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", IntegerType),
    StructField("Cylinders", IntegerType),
    StructField("Displacement", IntegerType),
    StructField("Horsepower", IntegerType),
    StructField("Weight_in_lbs", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  /*
    Reading a DF:
    - format
    - schema or option("inferSchema", "true")
    - path
    - zero or more options
  */
  var carsDf = spark.read
    .format("json")
    .schema(carsSchema) // enforce the schema
    .option("mode", "failFast") // dropMalformed, permissive
    .option("path", "src/main/resources/data/cars.json") // same as load(path)
    .load()

  carsDf.show()

  // Alternative reading with options map
  var carsDfWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  carsDfWithOptionMap.printSchema()

  /*
    Writing DFs
    - format
    - save mode = overwrite, append, ignore, errorIfExists
    - path
    - zero or more options
  */
  carsDf.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("c:/sparks_output/cars_dupe.json")

  val carsWithDateSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", IntegerType),
    StructField("Cylinders", IntegerType),
    StructField("Displacement", IntegerType),
    StructField("Horsepower", IntegerType),
    StructField("Weight_in_lbs", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  // JSON flags
  spark.read
    .schema(carsWithDateSchema)
    .option("dateFormat", "YYYY-MM-dd") // works only with schema; if spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // uncompressed is default; other are bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .schema(stocksSchema)
    .options(Map(
      "dateFormat" -> "MMM dd YYYY",
      "header" -> "true", // ignores the first row
      "sep" -> ",", // separators
      "nullValue" -> ""
    ))
    .csv("src/main/resources/data/stocks.csv")

  /*
    Parquet
    - higher compression power
    - the default writing mode
  */
  carsDf.write
    .mode(SaveMode.Overwrite)
    .save("c:/sparks_output/cars.parquet")

  // Reading from a remove DB
  val employeesDf = spark.read
    .format("jdbc")
    .options(Map(
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
      "user" -> "docker",
      "password" -> "docker",
      "dbtable" -> "public.employees"
    ))
    .load()

  employeesDf.show()
}
