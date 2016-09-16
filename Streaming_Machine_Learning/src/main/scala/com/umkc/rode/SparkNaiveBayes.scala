package com.umkc.rode


import java.net.InetAddress
import java.nio.file.{Files, Paths}

import com.umkc.rode.NLPUtils._
import com.umkc.rode.Utils._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by Mayanka on 14-Jul-15.
 */
object SparkNaiveBayes {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "F:\\winutils")
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkNaiveBayes").set("spark.driver.memory", "3g").set("spark.executor.memory", "3g")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val sc = ssc.sparkContext
    val stopWords = sc.broadcast(loadStopWords("/stopwords.txt")).value
    val labelToNumeric = createLabelMap("data2/training/")
    var model: NaiveBayesModel = null
    val PORT_NUMBER = 9999
    // Training the data
    if (!Files.exists(Paths.get("data2/model/NB"))) {

      val training = sc.wholeTextFiles("data2/training/*")
        .map(rawText => createLabeledDocument(rawText, labelToNumeric, stopWords))
      val X_train = tfidfTransformer(training)
      model = NaiveBayes.train(X_train, lambda = 1.0)
      model.save(sc, "data2/model/NB")
    }
    else {
      model = NaiveBayesModel.load(sc, "data2/model/NB")
    }

    // Get IP Address of the Machine
    println("IP ADDRESS : :   " + socket.findIpAdd())

    // Socket open for Testing Data
    lazy val address: Array[Byte] = Array(10.toByte, 192.toByte, 0.toByte, 47.toByte)
    val ia = InetAddress.getByAddress(address)

    val lines = ssc.socketTextStream(ia.getHostName, PORT_NUMBER, StorageLevel.MEMORY_ONLY)
    val lines=sc.wholeTextFiles("data2/training/sci.space/59497")
    val data = lines.map(line => {
      if(line.length>0) {
        val test = createLabeledDocumentTest(line, labelToNumeric, stopWords)
        println(test.body)
        test.body
      }
      else
        null
    })

    if(data!=null) {
      data.foreachRDD(rdd => {
        val X_test = tfidfTransformerTest(sc, rdd)
        val predictionAndLabel = model.predict(X_test)
        println("PREDICTION")
        predictionAndLabel.foreach(x => {
          labelToNumeric.foreach { y => if (y._2 == x) {
            println(y._1)
            socket.sendCommandToRobot(y._1)
          }
          }
        })
      }
      )
    }
    ssc.start()
    ssc.awaitTermination()

    // val accuracy = 1.0 *  predictionAndLabel.filter(x => x._1 == x._2).count() / X_test.count()

    /*  println("*************Accuracy Report:***********************")
   */
    //   println(accuracy)
    //evaluateModel(predictionAndLabel,"Naive Bayes Results")


  }


}
