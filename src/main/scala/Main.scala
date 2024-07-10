import org.apache.spark.sql.SparkSession

@main def hello(): Unit =
  println("Hello world!")

  val spark = SparkSession
    .builder()
    .appName("Spark SQL Learner")
    .getOrCreate()

  val path = "/src/population_2024.csv"

  val df = spark.read.csv(path)

  df.show()


