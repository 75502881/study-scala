package study.trex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author ${user.name}
 */
object WordCountStream {

  def main(args: Array[String]) {
        var conf = new SparkConf().setAppName("WordCount").setMaster("spark://192.168.31.231:7077")
        var sc = new SparkContext(conf)
        var lines = sc.textFile("hdfs://192.168.31.231:9000/spark.txt", 1)
        var words = lines.flatMap(line ⇒ line.split(" "))
        val pairs = words.map(word ⇒ (word, 1))
        val wordCount = pairs.reduceByKey(_ + _)
        wordCount.foreach(wordCount ⇒ println(wordCount._1+" appeared "+ wordCount._2 + " times "))

  }

}
