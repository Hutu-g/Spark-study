package org.scala.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 序列化
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object action_02_xuliehua {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc: SparkContext = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val user: User = new User
    //闭包检测
    rdd.foreach(num => {
      println("age=" + (user.age + num))
    })

    sc.stop()
  }
//  class User extends Serializable { //继承Serializable
  class User(){ //样例类在编译时，会自动混入序列化特质（实现可序列化接口）
    var age: Int = 30
  }

}
