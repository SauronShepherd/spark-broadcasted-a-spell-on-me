import org.apache.spark.sql.{Encoders, Row, SparkSession}

import scala.util.Random

object FirstTry {

  def main(args: Array[String]): Unit = {

    // 1. Configure Spark
    val spark = SparkSession.builder.master("local").getOrCreate()

    import spark.implicits._

    // 2. Define the function to generate random records
    def generateRandomRecord(seed: Long): (Int, Double) = {
      val rand = new Random(seed)
      (rand.nextInt(), rand.nextDouble())
    }
    // 3. Create the big table dataframe
    val numRows = 3000000
    val bigRDD = spark.sparkContext.parallelize(1 to numRows, numSlices = 2)
      .map(seed => generateRandomRecord(seed))
    val bigDF = bigRDD.toDF("key", "value")

    // 4. Create the small table dataframe
    val smallDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], bigDF.schema)

    // 5. Join
    val joinDF = bigDF.join(smallDF, "key")

    // 6. Count, sleep and stop
    joinDF.count()
    Thread.sleep(60000)
    spark.stop()
  }
}
