package org.demos

import org.apache.spark.sql.functions.{col, count, trim}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType}
import com.audienceproject.spark.dynamodb.implicits._

object SparkDDB {

  def main(args: Array[String]): Unit = {

    println("SparkDDB is running...")

    if (args.length < 4) {
      println("Usage: SparkDDB <inputRegion> <inputTableName> <outputRegion> <outPutTableName>")
      System.exit(-1)
    }

    val inputRegion = args(0)
    val inputTableName = args(1)

    val outputRegion = args(2)
    val outPutTableName = args(3)

    /*
    Initialize spark.
     */

    val spark = SparkSession.builder()
      .appName("SparkDDB")
      .getOrCreate()

    /*
    Read records from DynamoDB table.
     */

    val originSchema = StructType(Array(
      StructField("id", StringType),
      StructField("name", StringType)
    ))

    val recordsDF = spark.read.option("region", inputRegion).schema(originSchema).dynamodb(inputTableName)

    /*
    Process data.
     */

    val resultDF = recordsDF.groupBy(col("id")).agg(count("name").as("count"))

    /*
    Write result to DynamoDB table.
     */

    resultDF.write.option("region", outputRegion).dynamodb(outPutTableName)

    spark.stop()

    println("SparkDDB is running...Done.")

  }

}
