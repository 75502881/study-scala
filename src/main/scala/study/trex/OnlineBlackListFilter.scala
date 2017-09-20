package study.trex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import scalaz.Split

/**
 * @author ${user.name}
 */
object OnlineBlackListFilter {

  def main(args: Array[String]) {
    var conf = new SparkConf().setAppName("OnlineBlackListFilter").setMaster("spark://192.168.31.231:7077")

    var scc = new StreamingContext(conf, Seconds(300))

    var blackList = Array(("hadoop", true), ("mahout", true))
    val blackListRdd = scc.sparkContext.parallelize(blackList, 8)

    var adsClickStream = scc.socketTextStream("192.168.31.231", 9999)
    val adsClickedStreamFormatted = adsClickStream.map { ads => (ads.split(" ")(1), ads) }
    adsClickedStreamFormatted.transform(userClickedRdd => {
      var joinedBlackListRdd = userClickedRdd.leftOuterJoin(blackListRdd)
      var validClicked = joinedBlackListRdd.filter(joinedItem => {
        if (joinedItem._2._2.getOrElse(false)) { false } else { true }
      })
      validClicked.map(validClick => validClick._2._1)
    }).print

    scc.start()
    scc.awaitTermination()
  }

}
