package com.umkc.sparkML

import java.io.{FileWriter, BufferedWriter, File}


object Utils {

  def getFileTree(f: File): Stream[File] =
    f #:: (if (f.isDirectory) f.listFiles().toStream.flatMap(getFileTree)
           else Stream.empty)

  def getFileAndParent(path: String): Stream[(String,String)] = {
    val f = new File(path)
    getFileTree(f) map (x => (x.getName, x.getParentFile.getName))
  }

  def getParent(path: String): Stream[String] = {
    val f = new File(path)
    getFileTree(f) map (x => (x.getParentFile.getName))
  }

  def createLabelMap(path: String): Map[String, Int] = {
    val x = getParent(path).toList.distinct
    (x zip x.indices).toMap
  }

  def getLabelandId(path: String):(String, String) = {
    val spath = path.split("/")
    val label = spath.init.last
    val id = spath.last
    (label, id)
  }
  def printToFile(pathName: String, fileName: String, contents: String) = {
    val file = new File(pathName + "/" + fileName + ".txt")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(contents)
    bw.close()
  }




}
