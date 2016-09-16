package com.umkc.rode
import java.io._
import java.net._
/**
 * Created by Mayanka on 20-Jul-15.
 */
object socket {
  def sendCommandtoRobot(string: String)
  {
    // Simple server

    try {


      lazy val address: Array[Byte] = Array(10.toByte, 205.toByte, 0.toByte, 181.toByte)
      val ia = InetAddress.getByAddress(address)
      val socket = new Socket(ia, 1234)
      val out = new PrintStream(socket.getOutputStream)
      //val in = new DataInputStream(socket.getInputStream())

      out.print(string)
    out.flush()

    out.close()
      //in.close()
      socket.close()
    }
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
    }

}
