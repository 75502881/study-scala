package study.trex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object DailyTop3Keyword {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder().config("spark.master", "local[*]")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

      
      var dataRdd = spark.read.text("")
     
      
      
    
  }
}
