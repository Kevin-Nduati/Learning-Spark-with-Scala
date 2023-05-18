// Let's write a spark program that reads a file with over 100,000 entries and computes and aggregates the counts for each color and state. These aggreagated counts tell us the 
// colors of M&Ms favored by students in each state

package main.scala.chapter2

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object MnMcount {
    def main(args: Array[String]) {
        val spark = SparkSession
            .builder
            .appName("MnMCount")
            .getOrCreate()

        if (args.length < 1) {
            print("Usage: MnMcount <mnm_file_dataset>")
            sys.exit(1)
        }
        // get the M&M data set file name
        val mnmFile = args(0)

        // read the file into a spark dataframe
        val mnmDF = spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(mnmFile)

        mnmDF.show(5, false)

        // aggregate count of all colors and groupBy state and color
        // orderBy descending order
        val countMnMDF = mnmDF.select("State", "Color", "Count")
            .groupBy("State", "Color")
            .sum("Count")
            .orderBy(desc("sum(Count)"))

        // show all the resulting aggregation for all the dates and colors

        countMnMDF.show(60)
        println(s"Total  Rows = ${countMnMDF.count()}")

        // Find the aggregate count for california by filtering

        val CaCountMnMDF = mnmDF.select("State", "Color", "Count")
            .where(col("State") === "CA")
            .groupBy("State", "Color")
            .sum("Count")
            .orderBy(desc("sum(Count)"))

        CaCountMnMDF.show(10)

        spark.stop()

    }
  
}
