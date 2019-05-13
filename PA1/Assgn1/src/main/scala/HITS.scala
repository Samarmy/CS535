import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object HITS {
    def main(args: Array[String]) {
      val spark = SparkSession.builder.appName("HITS").getOrCreate()
      val sc = spark.sparkContext
      import spark.implicits._

      var links2 = spark.read.textFile("/pa1/links.txt").map(line => line.split(":")).flatMap(pair => pair(1).trim.split(" ").map(x => (pair(0).toInt,x.toInt))).rdd.persist()
      val titles = spark.read.textFile("/pa1/titles.txt").rdd.persist()
      val rootSet = titles.zipWithIndex.map(x => (x._2.toInt+1, x._1)).persist()
      val rootSet2 = titles.zipWithIndex.filter(x => x._1.contains(args(0))).map(x => (x._2.toInt+1, x._1)).persist()
      val links = links2.join(rootSet2).map(x => (x._2._1, x._1))


      val partialBaseSet1 = rootSet.join(links).flatMap{case (k,v) => Array(k, v._2)}.persist()
      val reversedLinks = links.map(_.swap)
      val partialBaseSet2 = rootSet.join(reversedLinks).flatMap{case (k,v) => Array(k, v._2)}.persist()
      val baseSet = (partialBaseSet1 ++ partialBaseSet2).distinct().persist()
      
      var hubScores = links.keys.map(x => (x,1.0)).persist()
      var authScores = reversedLinks.keys.map(x => (x,1.0)).persist()
      
      var notConverged = true
      for (i <- 1 to 5){
        val unnormAuthScores = hubScores.join(links).map{case (k,v) => (v._2,v._1)}.reduceByKey(_+_).persist()
        var authSum = unnormAuthScores.values.sum()
        authScores = unnormAuthScores.map{case (k,v) => (k, v/authSum)}.persist()
        
        val unnormHubScores = authScores.join(reversedLinks).map{case (k,v) => (v._2,v._1)}.reduceByKey(_+_).persist()
        var hubSum = unnormHubScores.values.sum()
        hubScores = unnormHubScores.map{case (k,v) => (k, v/hubSum)}.persist()
      }
      
      rootSet.sample(false, 0.1).map{case (k,v) => v}.saveAsTextFile("/pa1/rootset")
      
      val kvBaseSet = baseSet.map(x => (x,0))
      titles.zipWithIndex.map(x => (x._2.toInt+1, x._1)).join(kvBaseSet).map{case (k,v) => v._1}.saveAsTextFile("/pa1/baseset")
      
      sc.parallelize(titles.zipWithIndex.map(x => (x._2.toInt+1, x._1)).join(hubScores).values.sortBy(-_._2).take(50)).saveAsTextFile("/pa1/hubscores")
      sc.parallelize(titles.zipWithIndex.map(x => (x._2.toInt+1, x._1)).join(authScores).values.sortBy(-_._2).take(50)).saveAsTextFile("/pa1/authscores")
      
      spark.stop()
    }
}
