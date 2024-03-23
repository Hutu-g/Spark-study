package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 转换算子map
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_01_map {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc: SparkContext = new SparkContext(sparkConf)
    //1.map()
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //    //转换函数
    //    def mapFunc(num: Int): Int = {
    //      num * 2
    //    }

    //map映射转换
    //val mapRdd: RDD[Int] = rdd.map(_ * 2)//转换值
    val mapRdd: RDD[String] = rdd.map(_ + "哈哈哈")//转换类型
    mapRdd.collect.foreach(println)

    sc.stop()
  }

}
