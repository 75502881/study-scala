package study.trex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap

object BestSaleInfo2 {
  def main(args: Array[String]) {

    val conditionMap = Map("city" -> Array("beijing"), "platform" -> Array("android"), "version" -> Array("1.0", "1.2", "1.5", "2.0"))

    var platform = "android"
    var platforms = conditionMap.get("platforms");

    println(platforms.getOrElse(Array()).contains(platform))

    val list = "apple" ::"text" :: "banana" :: 1 :: 2 :: Nil

    val strings = list.filter( {
      case row if(row=="apple") => true
      case d if(d=="text") => true
      case _         => false
    })
    
    strings.foreach(println(_))
  }
}
