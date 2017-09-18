package study.trex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import scalaz.Split

/**
 * @author ${user.name}
 */
object WordCountStream {

  def main(args: Array[String]) {
    var conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    var scc = new StreamingContext(conf, Seconds(300))
    var lines = scc.socketTextStream("192.168.31.231", 9999)
    var words = lines.flatMap(_.split(" "))
    var wordCount = words.map { x => (x, 1) }.reduceByKey(_ + _)
    wordCount.foreachRDD(rdd => rdd.foreachPartition(recode => recode.foreach(f=>println("word:"+f._1+ " count:"+f._2))))

    scc.start()
    scc.awaitTermination()
  }

}
