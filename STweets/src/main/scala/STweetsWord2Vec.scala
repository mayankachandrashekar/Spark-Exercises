import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object STweetsWord2Vec {


  def main(args: Array[String]) {
    StreamingExamples.setStreamingLogLevels()
    System.setProperty("hadoop.home.dir","F:\\winutils");

    //Create a spark configuration with a custom name and master
    // For more master configuration see  https://spark.apache.org/docs/1.2.0/submitting-applications.html#master-urls
    val sparkConf = new SparkConf().setAppName("STweetsWord2Vec").setMaster("local[*]")
    //Create a Streaming COntext with 2 second window
    val sc= new SparkContext(sparkConf);
    val documents: RDD[Seq[String]] = sc.textFile("text8").map(_.split(" ").toSeq)
    val word2vec = new Word2Vec()

    val model = word2vec.fit(documents)

    val synonyms = model.findSynonyms("japan", 40)

    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    // Save and load model
//    model.save(sc, "myModelPath")
//    val sameModel = Word2VecModel.load(sc, "myModelPath")
//    print("")
    //Using the streaming context, open a twitter stream (By the way you can also use filters)
    //Stream generates a series of random tweets

  }
}
