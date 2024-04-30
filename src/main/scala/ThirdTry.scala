import org.apache.spark.sql.{Row, SparkSession}

object ThirdTry {


  def main(args: Array[String]): Unit = {
    // 1. Configure Spark
    val spark = SparkSession.builder
                              .master("local")
                              .getOrCreate()

    // 2. Load the big table dataframe and save it using parquet format with snappy compression
    spark.read.option("header","true").csv("src/main/resources/people-500000.csv")
         .write.format("parquet").option("compress","snappy").save("src/main/resources/big_table")
    val bigDF = spark.read.load("src/main/resources/big_table")
    println(s"bigDF sizeInBytes: ${bigDF.queryExecution.analyzed.stats.sizeInBytes}")

    // 3. Increase the threshold for autobroadcast
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", bigDF.queryExecution.analyzed.stats.sizeInBytes.toLong)

    // 4. Load the small table dataframe
    val smallDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], bigDF.schema)
    println(s"smallDF sizeInBytes: ${smallDF.queryExecution.analyzed.stats.sizeInBytes}")

    // 5. Join and count by index
    val joinByIndexDF = bigDF.join(smallDF, "index")
    joinByIndexDF.count()

    // 6. Join and count by all fields
    try {
      val joinByAllDF = bigDF.join(smallDF, bigDF.columns.toSet.intersect(smallDF.columns.toSet).toList)
      joinByAllDF.count()
    } catch {
      case ex => ex.printStackTrace()
    }

    // 7. Sleep and stop
    Thread.sleep(60000)
    spark.stop()
  }
}
