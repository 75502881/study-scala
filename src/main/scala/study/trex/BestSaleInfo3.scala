package study.trex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
import scala.util.control.Breaks._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType

object BestSaleInfo3 {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder().config("spark.master", "local[*]")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._
    import spark.sql
    val conditionMap = new scala.collection.mutable.HashMap[String, Array[String]]
    conditionMap.put("city", Array("beijing"));
    conditionMap.put("platform", Array("android"));
    conditionMap.put("version", Array("1.0", "1.5", "2.0"));

    var queryParamMapBroadcast = spark.sparkContext.broadcast(conditionMap)

//    var saleInfoRdd = spark.read.csv("hdfs://192.168.31.231:9000/user/root/input/sales.csv")
    
    var saleInfoRdd = spark.read.format("com.databricks.spark.csv").option("header", true).option("inferSchema", true).load("hdfs://192.168.31.231:9000/user/root/input/sales.csv")


    saleInfoRdd.printSchema()
    //var saleFilterRdd = filterSaleInfo(saleInfoRdd, queryParamMapBroadcast)
    var saleFilterRdd = saleInfoRdd.filter(row => isValidDataRec(row, queryParamMapBroadcast))

   
    saleFilterRdd.foreach(f=>println(f.toString()))
  }

  def filterSaleInfo(saleInfoRdd: RDD[String], queryParamMapBroadcast: Broadcast[HashMap[String, Array[String]]]): RDD[String] = {
    saleInfoRdd.filter(row => {
      var isFilter = true
      var logSlipted = row.split("\t")
      var city = logSlipted(3)
      var platform = logSlipted(4)
      var version = logSlipted(5)

      var queryParamMap = queryParamMapBroadcast.value

      for (cityPara <- queryParamMap.get("city")) {
        if (!cityPara.contains(city)) {
          isFilter = false
        }
      }

      if (!queryParamMap.get("city").getOrElse(Array()).contains(city)) {
        isFilter = false

      }
      if (!queryParamMap.get("platform").getOrElse(Array()).contains(platform)) {
        isFilter = false

      }
      if (!queryParamMap.get("version").getOrElse(Array()).contains(version)) {
        isFilter = false
      }
      isFilter
    })
  }

  def isValidDataRec(row: Row, queryParamMapBroadcast: Broadcast[HashMap[String, Array[String]]]) = {
   
    var city = row(3)
    var platform = row(4)
    var version = row(5).toString()
    var queryParamMap = queryParamMapBroadcast.value

    row match {
      case _ if !queryParamMap.get("city").getOrElse(Array()).contains(city) => false
      case _ if !queryParamMap.get("platform").getOrElse(Array()).contains(platform) => false
      case _ if !queryParamMap.get("version").getOrElse(Array()).contains(version) => false
      case _ => true
    }
  }
}
