package com.umkc.rode


import java.nio.file.{Files, Paths}

import com.umkc.rode.NLPUtils._
import com.umkc.rode.Utils._
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Mayanka on 14-Jul-15.
 */
object SparkNaiveBayes {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "F:\\winutils")

    //Spark Context
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkNaiveBayes").set("spark.driver.memory", "3g").set("spark.executor.memory", "3g")
    val sc = new SparkContext(sparkConf)

    //Stopwords are broadcast to all RDD's across the cluster
    val stopWords = sc.broadcast(loadStopWords("/stopwords.txt")).value
    val labelToNumeric = createLabelMap("data/training/")

    var model:NaiveBayesModel=null


    //Train and create Model
    if (!Files.exists(Paths.get("data/model/NB"))) {

      val training = sc.wholeTextFiles("data/training/*")
        .map(rawText => createLabeledDocument(rawText, labelToNumeric, stopWords))
      val X_train = tfidfTransformer(training)
      model = NaiveBayes.train(X_train, lambda = 1.0)
      model.save(sc,"data/model/NB")
    }
    else
    {
      model=NaiveBayesModel.load(sc,"data/model/NB")
    }

    val test = sc.wholeTextFiles("data/test/*").map(rawText => createLabeledDocumentTest(rawText, labelToNumeric, stopWords))


    val X_test = tfidfTransformerTest(test)

    //Train / Predict
    val predictionAndLabel = X_test.map(x => (model.predict(x)))
    predictionAndLabel.foreach(x => {
      labelToNumeric.foreach { y => if (y._2 == x) {
        println(y._1)
        socket.sendCommandtoRobot(y._1)
      }
      }
    })

  }


}
