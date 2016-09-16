import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.{SparkConf, SparkContext}

object STweetsPCA {


  def main(args: Array[String]) {
    StreamingExamples.setStreamingLogLevels()
    System.setProperty("hadoop.home.dir","F:\\winutils");

    //Create a spark configuration with a custom name and master
    // For more master configuration see  https://spark.apache.org/docs/1.2.0/submitting-applications.html#master-urls
    val sparkConf = new SparkConf().setAppName("STweetsPCA").setMaster("local[*]")
    //Create a Streaming COntext with 2 second window
    val sc= new SparkContext(sparkConf);
    // Load and parse the data file.
    val rows = sc.textFile("sample_lda_data.txt").map { line =>
      val values = line.split(' ').map(_.toDouble)
      Vectors.dense(values)
    }
    val mat = new RowMatrix(rows)

    // Compute principal components.
    val pc = mat.computePrincipalComponents(mat.numCols().toInt)

    println("Principal components are:\n" + pc)

  }
}
