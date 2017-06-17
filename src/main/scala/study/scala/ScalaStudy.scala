package study.scala

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

object ScalaStudy {
  def main(args: Array[String]) {
    val conditionMap = new scala.collection.mutable.HashMap[String, Array[String]]
    conditionMap.put("city", Array("beijing"));
    conditionMap.put("city2", Array());
    conditionMap.put("platform", Array("android"));
    conditionMap.put("version", Array("1.0", "1.5", "2.0"));
    
    
    for (cityPara <- conditionMap.get("city2")) {
      println(cityPara.head)
    }

  }

}


