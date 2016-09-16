import java.io._
import java.net._

/**
 * Created by Mayanka on 20-Jul-15.
 */
object iOSConnector {
  def findIpAdd():String =
  {
    val localhost = InetAddress.getLocalHost
    val localIpAddress = localhost.getHostAddress

    return localIpAddress
  }
  def sendCommandToRobot(string: String)
  {
    // Simple server

    try {


      val ip = InetAddress.getByName("10.182.0.192").getHostName
      val socket = new Socket(ip, 5555)
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
