import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vector

import scala.collection.immutable.HashMap

object STweetsTF_IDF {


  def main(args: Array[String]) {
    StreamingExamples.setStreamingLogLevels()
    System.setProperty("hadoop.home.dir","F:\\winutils");

    //Create a spark configuration with a custom name and master
    // For more master configuration see  https://spark.apache.org/docs/1.2.0/submitting-applications.html#master-urls
    val sparkConf = new SparkConf().setAppName("STweetsTF_IDF").setMaster("local[*]")
    //Create a Streaming COntext with 2 second window
    val sc= new SparkContext(sparkConf);
    val documents: RDD[Seq[String]] = sc.textFile("README.txt").map(_.split(" ").toSeq)
    documents.foreach(f=>println(f))
    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)
    //tf.cache()

    tf.foreach(v=>println(v))
   // tf.saveAsTextFile("outputtf")
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)
    tfidf.coalesce(3,true)
    tfidf.foreach(vv=>println(vv))

    val map2=new HashMap[String,String]()
    val i="Element"
    val jj=tfidf.toLocalIterator
    var j=0
    


  // tfidf.saveAsTextFile("outputt")
    //Using the streaming context, open a twitter stream (By the way you can also use filters)
    //Stream generates a series of random tweets

  }
}
