package study.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TransformationFiter {
  def main(args: Array[String]) {
    var conf = new SparkConf().setAppName("TransformationFiter ").setMaster("local")
    var sc = new SparkContext(conf)
    var numbers = Array(1, 2, 3, 4, 5, 6,7,8,9,10)
    var numberRdd = sc.parallelize(numbers, 1)

    var multNumberRdd = numberRdd.filter(num⇒num%2==0)
    multNumberRdd.foreach(num⇒println(num))
  }
}