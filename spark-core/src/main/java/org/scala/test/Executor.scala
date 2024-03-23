package org.scala.test

import java.io.InputStream
import java.net.{ServerSocket, Socket}

/**
 *
 * @Author: hutu
 * @Date: 2024/3/17 16:23
 */
object Executor {
  def main(args: Array[String]): Unit = {
    //启动服务器，接受数据
    val server: ServerSocket = new ServerSocket(9999)
    println("服务器启动：等待数据")

    val client: Socket = server.accept()
    val in: InputStream = client.getInputStream
    val i: Int = in.read()
    println("接收到数据为：" + i)

    in.close()
    client.close()
    server.close()
  }
}
