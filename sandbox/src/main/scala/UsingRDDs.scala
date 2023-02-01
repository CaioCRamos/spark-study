import core.SparkApplication

object UsingRDDs extends SparkApplication {
  case class Car(Name: String,
                 Miles_per_Gallon: Option[Double],
                 Cylinders: Long,
                 Displacement: Double,
                 Horsepower: Option[Long],
                 Weight_in_lbs: Long,
                 Acceleration: Double,
                 Year: String,
                 Origin: String)

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/cars.json")

  import spark.implicits._

  val carsDS = carsDF.as[Car]
  val carsRDD = carsDS.rdd

  carsRDD.map(x => println(x.Name)).count()
}
