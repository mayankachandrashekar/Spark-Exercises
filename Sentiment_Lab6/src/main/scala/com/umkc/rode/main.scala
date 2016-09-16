package com.umkc.rode

import scala.io.Source
import java.io._


/**
 * Created by Bharath Attaluri on 17-07-2015.
 */
object main {
  def main(args: Array[String]) {
    val filename = "E:\\Guru_spark\\Sentiment_Lab6\\Inputfiles\\amazon_cells_labelled.txt"
    var i = 0
    var j=0
    for (line <- Source.fromFile(filename).getLines()) {
      //println(line)
      //line take (line.length - 1) mkString
      val value = line.slice(line.length-1 ,line.length)
      if (value == "0")
        {
          val file = new File("data\\negative\\"+i)
          val bw = new BufferedWriter(new FileWriter(file))
          bw.write(line)
          bw.close()
          i=i+1
        }
      else if(value == "1")
      {
        val file = new File("data\\positive\\"+j)
        val bw = new BufferedWriter(new FileWriter(file))
        bw.write(line)
        bw.close()
        j=j+1
      }
    }
  }
}
