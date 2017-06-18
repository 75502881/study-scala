package study.trex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TransformationFlatMap {
  def main(args: Array[String]) {
    var conf = new SparkConf().setAppName("TransformationFiter ").setMaster("local")
    var sc = new SparkContext(conf)
    var numberRdd = sc.textFile("/Users/oushuhua/test.txt",1)

    var multNumberRdd = numberRdd.flatMap(line⇒line.split(" "))
    multNumberRdd.foreach(num⇒println(num))
  }
}