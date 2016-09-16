An implementation of TF-IDF + a Naive Bayes Classifier using Apache Spark and Stanford NLP utils. 


- Clone the repo and cd into it
- Run `sbt assembly` to build uber jar 
- Submit by running  `spark-submit --class com.brokendata.NaiveBayesSpark target/scala-2.10/spark20newsgroup-assembly-1.0.jar`
from the repo's root. 

Make sure you have apache spark installed and in your $PATH, you will most likely need create a 
`$SPARK_HOME/conf/spark-defaults.conf` file and the following: 

`spark.executor.memory              3g`  
`spark.driver.memory                4g`
