package sparkJoins

import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.LazyLogging

object Main extends LazyLogging{

  def main(args: Array[String]): Unit = {
    logger.info("Spark joins testing")
    val spark = SparkSession.builder
      .appName("sparkJoins")
      .getOrCreate()
    
    val df = spark.read.format("csv")
      .option("header", "true")
      .load("training_data.csv")
      
    logger.info("Dataframe loaded")

    logger.info("-- Dataframe Schema --")
    df.printSchema()
    logger.info("-- Dataframe Show ---")
    df.show(10, false)

    logger.info("Finished!")
  }
}
