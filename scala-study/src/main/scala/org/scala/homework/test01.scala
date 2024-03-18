package org.scala.homework

/**
 *
 * @Author: hutu
 * @Date: 2024/3/6 8:09
 */
object test01 {
  def main(args: Array[String]): Unit = {

    val doubles: List[Double] = List(8000, 7000, 3.5, 5500, 6000, -1)
    val list: List[Double] = doubles.map(x => {
      if (x >= 1000)
        x
      else
        0
    })
    println(list)

  }




}
