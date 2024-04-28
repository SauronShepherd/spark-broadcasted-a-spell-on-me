import org.apache.spark.sql.SparkSession

object SecondTry {

  def main(args: Array[String]): Unit = {

    // 1. Configure Spark
    val spark = SparkSession.builder.master("local").getOrCreate()

    // 2. Load the big table dataframe
    val bigDF = spark.read.option("header","true").csv("src/main/resources/people-100000.csv")
    println(s"bigDF sizeInBytes: ${bigDF.queryExecution.analyzed.stats.sizeInBytes}")

    // 3. Load the small table dataframe
    val smallDF = spark.read.option("header","true").csv("src/main/resources/people-0.csv")
    println(s"smallDF sizeInBytes: ${smallDF.queryExecution.analyzed.stats.sizeInBytes}")

    // 4. Join
    val joinDF = bigDF.join(smallDF, "index")

    // 5. Count, sleep and stop
    joinDF.count()
    Thread.sleep(60000)
    spark.stop()
  }
}
