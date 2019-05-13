package test

import org.apache.spark.sql.SparkSession

object HITS {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
//      .master("local")
      .appName("Spark HITS")
      .getOrCreate()

    val lines = spark.read.textFile(args(0)).rdd
    val links = lines.flatMap{ s =>
      val Array(node, nodes) = s split ":"
      val items = nodes.trim.split("\\s+")
      items.map(item => (node.trim.toLong -> item.toLong))
    }
    val titles = spark.read.textFile(args(1)).rdd.zipWithIndex().map(x => (x._2+1, x._1))
    val links2 = titles.join(titles.join(links).values.map(_.swap)).values.filter(s => s._1.contains(if(args(2) == "NONE") "" else args(2)))
    val linksA = links2.map(_.swap).distinct().groupByKey().cache()
    val linksH = links2.distinct().groupByKey().cache()
    var authorities = linksA.mapValues(v => 1.0).cache()
    var hubs = linksH.mapValues(v => 1.0).cache()

    for (i <- 1 to args(3).toInt) {
      val contribsA = linksA.join(authorities).values.flatMap{ case (urls, rank) =>
        urls.map(url => (url, rank))
      }
      authorities = contribsA.reduceByKey(_ + _)
      val sumA = authorities.values.sum()
      authorities = authorities.mapValues(s => s/sumA)

      val contribsH = linksH.join(authorities).values.flatMap{ case (urls, rank) =>
        urls.map(url => (url, rank))
      }

      hubs = contribsH.reduceByKey(_ + _)
      val sumH = hubs.values.sum()
      hubs = hubs.mapValues(s => s/sumH)
    }

//    authorities.collect().foreach(println)
//    hubs.collect().foreach(println)
    spark.sparkContext.parallelize(authorities.sortBy(-_._2).take(50)).saveAsTextFile(args(4))
    spark.sparkContext.parallelize(hubs.sortBy(-_._2).take(50)).saveAsTextFile(args(5))

    spark.stop()
  }
}
