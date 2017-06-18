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
import org.apache.spark.sql.functions._

object BestSaleInfo {
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
    conditionMap.put("version", Array("1.0", "1.2", "1.5", "2.0"));

    var queryParamMapBroadcast = spark.sparkContext.broadcast(conditionMap)

    var saleInfoRdd = spark.sparkContext.textFile("hdfs://192.168.31.231:9000/user/root/input/sales.txt", 1)

    //var saleFilterRdd = filterSaleInfo(saleInfoRdd, queryParamMapBroadcast)
    var saleFilterRdd = saleInfoRdd.filter(row => isValidDataRec(row, queryParamMapBroadcast))

    var dateKeywordUserRDD = saleFilterRdd.map(row => {
      var logSlipted = row.split("\t")
      Tuple2(logSlipted(0) + "_" + logSlipted(2), logSlipted(1))
    })

    var dateKeywordUsersRDD = dateKeywordUserRDD.groupByKey()

    var dateKeywordUvRDD = dateKeywordUsersRDD.map(row => {
      Tuple2(row._1, row._2.toList.distinct.size)
    })

    println("---------------------------")

    // 将每天每个搜索词的uv数据，转换成DataFrame
    var dateKeywordUvRowRDD = dateKeywordUvRDD.map(row => {
      var dateKey = row._1.split("_")
      Row(dateKey(0), dateKey(1), row._2);
    })

    dateKeywordUvRowRDD.foreach(row => println(row.toString()))

    // The schema is encoded in a string
    val schemaString = "date keyword"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true)) :+ StructField("uv", IntegerType, nullable = true)

    val schema = StructType(fields)
    var dateKeywordUvDF = spark.createDataFrame(dateKeywordUvRowRDD, schema)

    dateKeywordUvDF.createOrReplaceTempView("daily_keyword_uv");

    var dailyTop3KeywordDF = sql("" + "SELECT date,keyword,uv " + "FROM (" + "SELECT " + "date,"
      + "keyword," + "uv," + "row_number() OVER (PARTITION BY date ORDER BY uv DESC) rank "
      + "FROM daily_keyword_uv" + ") tmp " + "WHERE rank<=3");

    dailyTop3KeywordDF.printSchema()

    var top3DateKeywordsRDD = dailyTop3KeywordDF.map(row => Tuple2(row.get(0).toString(), row.get(1).toString() + "_" + row.get(2)))
    .toDF("date", "keyUv").groupBy("date").agg(collect_list("keyUv"))
    
    top3DateKeywordsRDD.foreach(f=>println(f.toString()))
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

  def isValidDataRec(row: String, queryParamMapBroadcast: Broadcast[HashMap[String, Array[String]]]) = {
    var logSlipted = row.split("\t")
    var city = logSlipted(3)
    var platform = logSlipted(4)
    var version = logSlipted(5)
    var queryParamMap = queryParamMapBroadcast.value

    row match {
      case _ if !queryParamMap.get("city").getOrElse(Array()).contains(city) => false
      case _ if !queryParamMap.get("platform").getOrElse(Array()).contains(platform) => false
      case _ if !queryParamMap.get("version").getOrElse(Array()).contains(version) => false
      case _ => true
    }
  }
}
