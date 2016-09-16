package com.umkc.rode


import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.{SparkContext, SparkConf}
import NLPUtils._
import Utils._
import ModelEvaluation._

/**
 * Created by Mayanka on 14-Jul-15.
 */
object SparkNaiveBayes {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","F:\\winutils")
    val sparkConf =new SparkConf().setMaster("local[*]").setAppName("SparkNaiveBayes").set("spark.driver.memory", "3g").set("spark.executor.memory", "3g")
    val sc = new SparkContext(sparkConf)
    //Stopwords are broadcast to all RDD's across the cluster
    val stopWords = sc.broadcast(loadStopWords("/stopwords.txt")).value
    // map containing labels to numeric values for labeled Naive Bayes. "alt.atheism" -> 4
    val labelToNumeric = createLabelMap("data/training/")

    // tokenize, stem,
    val training = sc.wholeTextFiles("data/training/*")
      .map(rawText => createLabeledDocument(rawText, labelToNumeric, stopWords))
    val test = sc.wholeTextFiles("data/test/*")
      .map(rawText => createLabeledDocumentTest(rawText,labelToNumeric, stopWords))

    //create features
    val X_train = tfidfTransformer(training)
    val X_test = tfidfTransformerTest(test)

    //Train / Predict
    val model = NaiveBayes.train(X_train,lambda = 1.0)
    val predictionAndLabel = X_test.map(x => (model.predict(x)))
    predictionAndLabel.foreach(x=> {labelToNumeric.foreach(y=>if(y._2==x) println(y._1))})
   // val accuracy = 1.0 *  predictionAndLabel.filter(x => x._1 == x._2).count() / X_test.count()

  /*  println("*************Accuracy Report:***********************")
    println(accuracy)
    evaluateModel(predictionAndLabel,"Naive Bayes Results")
*/
  }


}
