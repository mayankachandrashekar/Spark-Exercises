import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object STweetsLDA {


  def main(args: Array[String]) {
    StreamingExamples.setStreamingLogLevels()
    System.setProperty("hadoop.home.dir","F:\\winutils");

    //Create a spark configuration with a custom name and master
    // For more master configuration see  https://spark.apache.org/docs/1.2.0/submitting-applications.html#master-urls
    val sparkConf = new SparkConf().setAppName("STweetsLDA").setMaster("local[*]")
    //Create a Streaming COntext with 2 second window
    val sc= new SparkContext(sparkConf);
    val data = sc.textFile("sample_lda_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble)))
    // Index documents with unique IDs
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()

    // Cluster the documents into three topics using LDA
    val ldaModel = new LDA().setK(3).run(corpus)

    // Output topics. Each is a distribution over words (matching word count vectors)
    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
    val topics = ldaModel.topicsMatrix
    for (topic <- Range(0, 3)) {
      print("Topic " + topic + ":")
      for (word <- Range(0, ldaModel.vocabSize)) { print(" " + topics(word, topic)); }
      println()
    }
    // Save and load model
//    model.save(sc, "myModelPath")
//    val sameModel = Word2VecModel.load(sc, "myModelPath")
//    print("")
    //Using the streaming context, open a twitter stream (By the way you can also use filters)
    //Stream generates a series of random tweets

  }

}
