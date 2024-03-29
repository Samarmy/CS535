import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Try
import java.util.Date
import java.lang.System.currentTimeMillis
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.feature.FeatureHasher
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.sql.functions._

object ML {
  class VertexProperty() extends Serializable
  case class tweet(hashtag:String,screen_name:String,time:Long,quote_count:Int,reply_count:Int,retweet_count:Int,favorite_count:Int,favorited:Boolean,retweeted:Boolean,filter_level:String) extends VertexProperty with Serializable
  case class user(screen_name:String,followers_count:Int,friends_count:Int,listed_count:Int,time:Long,favourites_count:Int,time_zone:String,geo_enabled:Boolean,verified:Boolean,statuses_count:Int,contributors_enabled:Boolean,is_translator:Boolean,profile_use_background_image:Boolean,default_profile:Boolean,default_profile_image:Boolean,following:Boolean,follow_request_sent:Boolean,notifications:Boolean,translator_type:String,labels:Map[String, Array[Long]],hashtags:Array[tweet],prediction:Map[String, Double]) extends VertexProperty with Serializable
  case class hash_data(screen_name:String,label:Int,followers_count:Int,friends_count:Int,listed_count:Int,time_user:Long,favourites_count:Int,time_zone:String,geo_enabled:Boolean,verified:Boolean,statuses_count:Int, contributors_enabled:Boolean, is_translator:Boolean,profile_use_background_image:Boolean,default_profile:Boolean, default_profile_image:Boolean, following:Boolean,follow_request_sent:Boolean, notifications:Boolean, translator_type:String, num_tweets:Int, num_user_relations:Int) extends Serializable
  case class test_data(screen_name:String,followers_count:Int,friends_count:Int,listed_count:Int,time_user:Long,favourites_count:Int,time_zone:String,geo_enabled:Boolean,verified:Boolean,statuses_count:Int, contributors_enabled:Boolean, is_translator:Boolean,profile_use_background_image:Boolean,default_profile:Boolean, default_profile_image:Boolean, following:Boolean,follow_request_sent:Boolean, notifications:Boolean, translator_type:String, num_tweets:Int, num_user_relations:Int) extends Serializable
    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("ML").config("spark.executor.extraJavaOptions","-Xss4g").config("driver-java-options","-Xss4g").getOrCreate()
        import spark.implicits._
        val sc = spark.sparkContext

        var userHashtags = spark.read.textFile("/tp/userHashtags/*").rdd.map(x => {
          var strAry = x.split(",")
          var props = strAry(1).substring(0, strAry(1).length - 1).split(" ")
          (strAry(0).substring(1), tweet(props(0), strAry(0).substring(1), props(1).toLong,  props(2).toInt, props(3).toInt, props(4).toInt, props(5).toInt, Try(props(6).toBoolean).getOrElse(false), Try(props(7).toBoolean).getOrElse(false), props(8)).asInstanceOf[Any])
        }).distinct().cache()

        val userData = spark.read.textFile("/tp/userData/*").rdd.map(x => {
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
          (x._1, x._2._1.asInstanceOf[user].copy(labels=Map(str -> Array(lng)), hashtags=Array(tweet_data)).asInstanceOf[Any])
        }).reduceByKey((v1, v2) => v1.asInstanceOf[user].copy(labels=(v1.asInstanceOf[user].labels ++ v2.asInstanceOf[user].labels.map{ case (k,v) => k -> (v ++ v1.asInstanceOf[user].labels.getOrElse(k,Array[Long]()))}), hashtags = (v1.asInstanceOf[user].hashtags ++ v2.asInstanceOf[user].hashtags))).cache()

        val userRelations = spark.read.textFile("/tp/userRelations/*").rdd.map(x => {
          var strAry = x.split(",")
          (strAry(0).substring(1), strAry(1).substring(0, strAry(1).length - 1))
        }).cache()

        val userVertices = userData.zipWithUniqueId().map(x => {
          val userName = x._1._1
          (userName, (x._2, x._1._2))
        }).cache()

        val userEdges = userRelations.join(userVertices).map(x => (x._2._1, x._2._2._1)).join(userVertices).map(x => Edge(x._2._1, x._2._2._1, "follows")).cache()

        val vertices: RDD[(VertexId, Any)] = userVertices.map(x => x._2).cache()

        val blank = user("None",0,0,0,0L,0,"None",false,false,0,false,false,false,false,false,false,false,false,"None",Map.empty[String, Array[Long]],Array[tweet](),Map[String, Double]())

        val defaultUser = (blank.asInstanceOf[Any])

        val graph = Graph(vertices, userEdges, defaultUser)

        val incomingHashMap = sc.broadcast(graph.collectNeighbors(EdgeDirection.Out).collectAsMap.toMap)

        val hashtag_ary = Array("politics", "funny", "win", "happybirthday", "metoo", "photography", "marvel", "pets", "friends", "science", "birthday", "tech", "technology", "fashion", "trump", "impeachdonaldtrump", "news", "fakenews", "family", "food", "summer", "usa", "love", "men", "women")

        def show(x: Option[Array[(org.apache.spark.graphx.VertexId, Any)]]) = x match {
              case Some(s) => s
              case None => Array[(org.apache.spark.graphx.VertexId, Any)]()
           }
       def show2(x: Option[Array[Long]]) = x match {
             case Some(s) => s
             case None => Array[Long]()
          }

        // var beggining_of_data = 1555912800000L
        // var beggining_of_data = 1557208800000L
        // var beggining_of_data = 1557122400000L
        var lookback_days = 3L
        var data = sc.emptyRDD[hash_data].persist(MEMORY_AND_DISK_SER)
        var data2 = sc.emptyRDD[test_data].persist(MEMORY_AND_DISK_SER)
        for(hashtag_str <- hashtag_ary){
          var lookback_time = currentTimeMillis()
          var temp_data = vertices.filter(x => x._2.asInstanceOf[user].labels.getOrElse(hashtag_str, Array[Long]()).length > 0)
          while(lookback_time > 1557208800000L){
            var temp_data_2 = temp_data.filter(x => {
               var boo = false
               for(z <- x._2.asInstanceOf[user].labels.getOrElse(hashtag_str, Array[Long]())){
                 if(z < (lookback_time - 86400000L)){
                   boo = true
                 }
               }
              boo
            }).map(x => {
              var user1 = x._2.asInstanceOf[user]
              var ary = user1.hashtags
              var label = 0
              var num_tw = 0
              var num_rel = 0
              for(h <- ary){
                if(h.hashtag == hashtag_str){
                  if(h.time < lookback_time && h.time >= (lookback_time - 86400000L)){
                    label = label + 1
                  }else if(h.time < (lookback_time - 86400000L) && h.time > (lookback_time - (86400000L*(3L + 1L) ))){
                    num_tw = num_tw + 1
                  }
                }
              }
              for(u <- show(incomingHashMap.value.get(x._1))){
                for(h2 <- u._2.asInstanceOf[user].hashtags){
                  if(h2.hashtag == hashtag_str && h2.time < (lookback_time - 86400000L) && h2.time > (lookback_time - (86400000L*(3L + 1L) ))){
                      num_rel = num_rel + 1
                  }
                }
              }
              hash_data(user1.screen_name, label, user1.followers_count, user1.friends_count, user1.listed_count, user1.time, user1.favourites_count, user1.time_zone, user1.geo_enabled, user1.verified, user1.statuses_count, user1.contributors_enabled, user1.is_translator, user1.profile_use_background_image, user1.default_profile, user1.default_profile_image, user1.following, user1.follow_request_sent, user1.notifications, user1.translator_type, num_tw, num_rel)
            }).cache()
            data = data ++ temp_data_2.cache()
            data.take(1)
            data.cache()
            lookback_time = lookback_time - 86400000L
          }

          var temp_data_3 = temp_data.map(x => {
            var user1 = x._2.asInstanceOf[user]
            var ary = user1.hashtags
            var num_tw = 0
            var num_rel = 0
            for(h <- ary){
              if(h.hashtag == hashtag_str){
                if(h.time > (currentTimeMillis() - (86400000L*3L))){
                  num_tw = num_tw + 1
                }
              }
            }
            for(u <- show(incomingHashMap.value.get(x._1))){
              for(h2 <- u._2.asInstanceOf[user].hashtags){
                if(h2.hashtag == hashtag_str && h2.time > (currentTimeMillis() - (86400000L*(3L) ))){
                    num_rel = num_rel + 1
                }
              }
            }
            test_data(user1.screen_name, user1.followers_count, user1.friends_count, user1.listed_count, user1.time, user1.favourites_count, user1.time_zone, user1.geo_enabled, user1.verified, user1.statuses_count, user1.contributors_enabled, user1.is_translator, user1.profile_use_background_image, user1.default_profile, user1.default_profile_image, user1.following, user1.follow_request_sent, user1.notifications, user1.translator_type, num_tw, num_rel)
          }).cache()
          data2 = data2 ++ temp_data_3
          data2.take(1)
          data2.cache()
        }
        data = data.repartition(10)
        data.take(1)
        data.cache()
        var training = data.toDF
        var testing = data2.toDF
        val indexer1 = new StringIndexer().setInputCol("time_zone").setOutputCol("time_zone_index")
        val indexer2 = new StringIndexer().setInputCol("translator_type").setOutputCol("translator_type_index")

        val hasher = new FeatureHasher()
        hasher.setInputCols("followers_count", "friends_count", "listed_count", "time_user", "favourites_count", "time_zone_index", "geo_enabled", "verified", "statuses_count", "contributors_enabled", "is_translator", "profile_use_background_image", "default_profile", "default_profile_image", "following", "follow_request_sent", "notifications", "translator_type_index", "num_tweets", "num_user_relations")
        hasher.setOutputCol("features")

        val rf = new DecisionTreeRegressor().setLabelCol("label").setFeaturesCol("features")

        val pipeline = new Pipeline().setStages(Array(indexer1, indexer2, hasher, rf))

        ///////This is for getting predictions for tomorrow

        // val model = pipeline.fit(training)
        // val predictions = model.transform(testing)
        // var finaltraining = indexer1.fit(training).transform(training)
        // finaltraining = indexer2.fit(training).transform(finaltraining)
        // finaltraining = hasher.transform(finaltraining)
        // val categoricalFeaturesInfo = Map[Int, Int]()
        // val impurity = "variance"
        // val maxDepth = 5
        // val maxBins = 32

        // val modelFinal = DecisionTree.trainRegressor(finaltraining, categoricalFeaturesInfo, impurity,
        //   maxDepth, maxBins)
        // predictions.rdd.saveAsTextFile("/tp/finalPredictions")
        // val treeModel = model.stages(3).asInstanceOf[DecisionTreeModel]
        // model.write.overwrite().save("/tp/pipelineModel")
        // pipeline.write.overwrite().save("/tp/pipeline")
        // val sameModel = DecisionTreeModel.load(sc, "tp/finalModel")
        ////////This is for getting rmse

        val Array(trainingData, testData) = training.randomSplit(Array(0.9, 0.1))
        //
        val model2 = pipeline.fit(trainingData)
        val predictions2 = model2.transform(testData)

        model2.write.overwrite().save("/tp/pipelineModel")
        pipeline.write.overwrite().save("/tp/pipeline")
        // val sameModel = PipelineModel.load("/tmp/spark-logistic-regression-model")
        // model.transform(testing)
        //
        val df2 = predictions2.withColumn("prediction2", round(expr("prediction")))

        df2.rdd.saveAsTextFile("/tp/finalPredictions2.1")

        val evaluator = new RegressionEvaluator()
          .setLabelCol("label")
          .setPredictionCol("prediction2")
          .setMetricName("rmse")

          val evaluator2 = new RegressionEvaluator()
            .setLabelCol("label")
            .setPredictionCol("prediction")
            .setMetricName("rmse")

        val rmse2 = evaluator2.evaluate(df2)
        val rmse = evaluator.evaluate(df2)

        sc.parallelize(Seq(s"Root Mean Squared Error (RMSE) on test data = $rmse2")).saveAsTextFile("/tp/rmse2.2")
        sc.parallelize(Seq(s"Root Mean Squared Error (RMSE) on test data = $rmse")).saveAsTextFile("/tp/rmse2.3")

        spark.stop()
    }
}
