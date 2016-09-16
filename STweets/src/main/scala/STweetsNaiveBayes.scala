import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object STweetsNaiveBayes {


  def main(args: Array[String]) {
    StreamingExamples.setStreamingLogLevels()
    System.setProperty("hadoop.home.dir","F:\\winutils");

    //Create a spark configuration with a custom name and master
    // For more master configuration see  https://spark.apache.org/docs/1.2.0/submitting-applications.html#master-urls
    val sparkConf = new SparkConf().setAppName("STweetsPCA").setMaster("local[*]")
    //Create a Streaming COntext with 2 second window
    val sc= new SparkContext(sparkConf);
    // Load and parse the data file.
    val data = sc.textFile("sample_naive_bayes_data.txt")
    val parsedData = data.map { line =>
      val parts = line.split(',')
      println(parts(0))
      println(parts(1).trim())
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).trim().split(' ').map(_.toDouble)))
    }
    // Split data into training (60%) and test (40%).
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)

    val model = NaiveBayes.train(training, lambda = 1.0)

    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println("Accuracy \n",accuracy)
    predictionAndLabel.collect()
   // println(predictionAndLabel)
    // Save and load model
    //model.save(sc, "myModelPath")
  //  val sameModel = NaiveBayesModel.load(sc, "myModelPath")

  }
}
