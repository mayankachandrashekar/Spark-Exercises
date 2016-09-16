package com.umkc.sparkML

import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by Mayanka on 16-Jul-15.
 */
object SparkStreamingKMeans {
  def main(args: Array[String]) {
    val filters = args

    System.setProperty("twitter4j.oauth.consumerKey", "XmuCJg6wqok0kM4atoBWyzX70")
    System.setProperty("twitter4j.oauth.consumerSecret", "M791X1Py0jy52DG2f18EsxS0CYaMJhOfEZykO8H3mOLmfMXOBD")
    System.setProperty("twitter4j.oauth.accessToken", "66398818-wqoEXxQRTtb5GS24eqvn4DS5yQHIfay0NkgN3YDed")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "xP3IHuIaGJAuDES88Mt6TuxVEz3oSDz5AlYOgtZ7MEZD1")

    val sparkConf = new SparkConf().setAppName("SparkStreamingML").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, filters)
//    if (args.length != 7) {
//      System.err.println(
//        "Usage: KMeans " +
//          "<inputDir> <outputDir> <batchDuration> <numClusters> <numDimensions> <halfLife> <batchUnit>")
//      System.exit(1)
//    }

    val (inputDir, outputDir, batchDuration, numClusters, numDimensions, halfLife, timeUnit) =
      ("data//training//", "data//", 3.0, 3, 3, 10,"batches")


    val trainingData = ssc.textFileStream(inputDir).map(Vectors.parse)

    val model = new StreamingKMeans()
      .setK(numClusters)
      .setHalfLife(halfLife, timeUnit)
      .setRandomCenters(numDimensions, 0.0)

    model.trainOn(trainingData)

    val predictions = model.predictOn(trainingData)

    predictions.foreachRDD { rdd =>
      val modelString = model.latestModel().clusterCenters
        .map(c => c.toString.slice(1, c.toString.length-1)).mkString("\n")
      val predictString = rdd.map(p => p.toString).collect().mkString("\n")
      val dateString = Calendar.getInstance().getTime.toString.replace(" ", "-").replace(":", "-")
      Utils.printToFile(outputDir, dateString + "-model", modelString)
      Utils.printToFile(outputDir, dateString + "-predictions", predictString)
    }

    ssc.start()
    ssc.awaitTermination()

  }

  }
