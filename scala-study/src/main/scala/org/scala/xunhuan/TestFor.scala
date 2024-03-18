package org.scala.xunhuan

/**
 *  for循环
 * @Author: hutu
 * @Date: 2024/3/3 11:25
 */
object TestFor {
  def main(args: Array[String]): Unit = {
    //范围数据循环（to）
    for (i <- 1 to 5) {
      println("to循环" + i)
    }
    println()
    //范围数据循环（Until）
    for(i <- 1 until 3) {
      println(i + "Until循环")
    }
    println()
    println()
    //循环守卫
    for (i <- 1 to 3 if i != 2) {
      print(i + " ")
    }
    for (i <- 1 to 3){
      if (i != 2) {
        print(i + " ")
      }
    }
    println()
    //嵌套循环
    for (i <- 1 to 3; j <- 1 to 3) {
      println(" i =" + i + " j = " + j)
    }
    println()
    //倒序打印
    for (i <- 1 to 10 reverse) {
      println(i)
    }


  }
}
