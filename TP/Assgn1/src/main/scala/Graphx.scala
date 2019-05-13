import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Try
import java.util.Date

class VertexProperty()
case class tweet(hashtag:String,screen_name:String,time:Long,quote_count:Int,reply_count:Int,retweet_count:Int,favorite_count:Int,favorited:Boolean,retweeted:Boolean,filter_level:String) extends VertexProperty
case class user(screen_name:String,followers_count:Int,friends_count:Int,listed_count:Int,time:Long,favourites_count:Int,time_zone:String,geo_enabled:Boolean,verified:Boolean,statuses_count:Int,contributors_enabled:Boolean,is_translator:Boolean,profile_use_background_image:Boolean,default_profile:Boolean,default_profile_image:Boolean,following:Boolean,follow_request_sent:Boolean,notifications:Boolean,translator_type:String,labels:Map[String, Array[Long]],hashtags:Array[tweet],prediction:Map[String, Double]) extends VertexProperty

object Graphx {
    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("Graphx").getOrCreate()
        import spark.implicits._
        val sc = spark.sparkContext

        // var times = Array(1555912800000L, 1555999200000L, 1556085600000L, 1556172000000L, 1556258400000L, 1556344800000L, 1556431200000L, 1556517600000L, 1556604000000L, 1556690400000L, 1556776800000L, 1556863200000L, 1556949600000L, 1557036000000L, 1557122400000L, 1557208800000L, 1557295200000L, 1557381600000L, 1557468000000L)

        var userHashtags = spark.read.textFile("/test/userHashtags/*").rdd.map(x => {
          var strAry = x.split(",")
          var props = strAry(1).substring(0, strAry(1).length - 1).split(" ")
          (strAry(0).substring(1), tweet(props(0), strAry(0).substring(1), props(1).toLong,  props(2).toInt, props(3).toInt, props(4).toInt, props(5).toInt, Try(props(6).toBoolean).getOrElse(false), Try(props(7).toBoolean).getOrElse(false), props(8)).asInstanceOf[Any])
        }).distinct().cache()
        //(maxgin42,happybirthday 1555969426304 0 0 0 0 False False low)
        val userData = spark.read.textFile("/test/userData/*").rdd.map(x => {
          var strAry = x.split(",")
          var props = strAry(1).substring(0, strAry(1).length - 1).split(" ")
          (strAry(0).substring(1), user(strAry(0).substring(1), props(0).toInt, props(1).toInt, props(2).toInt, props(3).split("\\.")(0).toLong, props(4).toInt, props(5), Try(props(6).toBoolean).getOrElse(false), Try(props(7).toBoolean).getOrElse(false), props(8).toInt, Try(props(9).toBoolean).getOrElse(false), Try(props(10).toBoolean).getOrElse(false), Try(props(11).toBoolean).getOrElse(false), Try(props(12).toBoolean).getOrElse(false), Try(props(13).toBoolean).getOrElse(false), Try(props(14).toBoolean).getOrElse(false), Try(props(15).toBoolean).getOrElse(false), Try(props(16).toBoolean).getOrElse(false), props(17), Map.empty[String, Array[Long]], Array[tweet](), Map.empty[String, Double]))
        }).reduceByKey((v1, v2) => {
          if(v1.asInstanceOf[user].time > v2.asInstanceOf[user].time){
            v1
          }else{
            v2
          }
        }).join(userHashtags).map(x => {
          var tweet_data = x._2._2.asInstanceOf[tweet]
          var str = tweet_data.hashtag
          var lng = tweet_data.time
          // user_data.hashtags :+ tweet_data
          (x._1, x._2._1.asInstanceOf[user].copy(labels=Map(str -> Array(lng)), hashtags=Array(tweet_data)).asInstanceOf[Any])
        }).reduceByKey((v1, v2) => v1.asInstanceOf[user].copy(labels=(v1.asInstanceOf[user].labels ++ v2.asInstanceOf[user].labels.map{ case (k,v) => k -> (v ++ v1.asInstanceOf[user].labels.getOrElse(k,Array[Long]()))}), hashtags = (v1.asInstanceOf[user].hashtags ++ v2.asInstanceOf[user].hashtags))).map(x => (x._1 + " u", x._2)).cache()

        userHashtags = userHashtags.map(x => (x._1 + " t", x._2))
        //(LeadingWPassion,50495 49713 282 1445416821000.0 15299 None False False 434339 False False True False False None None None none)
        val userRelations = spark.read.textFile("/test/userRelations/*").rdd.map(x => {
          var strAry = x.split(",")
          (strAry(0).substring(1), strAry(1).substring(0, strAry(1).length - 1))
        }).cache()

        val indexedData = userData.union(userHashtags).zipWithUniqueId().cache()

        val userVertices = indexedData.filter(x => x._1._1.contains(" u")).map(x => {
          val userName = x._1._1
          (userName.substring(0, userName.length - 2), (x._2, x._1._2))
        }).cache()

        val tweetVertices = indexedData.filter(x => x._1._1.contains(" t")).map(x => {
          val userName = x._1._1
          (userName.substring(0, userName.length - 2), (x._2, x._1._2))
        }).cache()

        val userEdges = userRelations.join(userVertices).map(x => (x._2._1, x._2._2._1)).join(userVertices).map(x => Edge(x._2._1, x._2._2._1, "follows")).cache()

        val tweetEdges = tweetVertices.join(userVertices).map(x => Edge(x._2._2._1, x._2._1._1, "tweeted")).distinct().cache()

        val vertices: RDD[(VertexId, Any)] = tweetVertices.map(x => x._2).union(userVertices.map(x => x._2))

        val edges = userEdges ++ tweetEdges

        val blank = "None"

        val defaultUser = (blank.asInstanceOf[Any])

        val graph = Graph(vertices, edges, defaultUser)

        spark.stop()
    }
}
