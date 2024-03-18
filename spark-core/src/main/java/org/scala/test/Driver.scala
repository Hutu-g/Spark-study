package org.scala.test

import java.io.OutputStream
import java.net.Socket

/**
 *
 * @Author: hutu-g
 * @Date: 2024/3/17 16:23
 */
object Driver {
  def main(args: Array[String]): Unit = {

    //连接 Executor
    val client: Socket = new Socket("localhost", 9999)
    val out: OutputStream = client.getOutputStream
    out.write(2)

    //关闭连接
    out.flush()
    out.close()
    client.close()
  }

}
