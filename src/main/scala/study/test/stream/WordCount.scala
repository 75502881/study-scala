package study.test.stream

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import study.trex.day._

/**
 * @author ${user.name}
 */
object WordCount {

  def main(args: Array[String]) {

    var obj = TestObj("sds", "s")
    var conf = new SparkConf().setAppName("WordCount").setMaster("spark://192.168.31.231:7077")
    var sc = new SparkContext(conf)
    var lines = sc.textFile("hdfs://192.168.31.231:9000/spark.txt", 1)
    var words = lines.flatMap(line ⇒ line.split(" "))
    val pairs = words.map(word ⇒ (word, 1))
    val wordCount = pairs.reduceByKey(_ + _)
    wordCount.foreach(wordCount ⇒ println(wordCount._1 + " appeared " + wordCount._2 + " times "))
    //
    //    val conf = new SparkConf()
    //      .setAppName("WordCount").setMaster("spark://192.168.31.231:7077");
    //    val sc = new SparkContext(conf)
    //
    //    var lines = sc.textFile("hdfs://192.168.31.231:9000/spark.txt", 1)
    //    val words = lines.flatMap { line => line.split(" ") }
    //    val pairs = words.map { word => (word, 1) }
    //    val wordCounts = pairs.reduceByKey { _ + _ }
    //
    //    wordCounts.foreach(wordCount => println(wordCount._1 + " appeared " + wordCount._2 + " times."))
  }

}
