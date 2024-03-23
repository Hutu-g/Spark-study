package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 并行测试
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_01_map_par {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc: SparkContext = new SparkContext(sparkConf)
    //1.map()
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    val mapRdd: RDD[Int] = rdd.map(num=>{
      println(">>>>>>>>" + num)
      num
    })
    val mapRdd1: RDD[Int] = mapRdd.map(num => {
      println("********" + num)
      num
    })//转换类型
    mapRdd1.collect()

    //1.rdd的计算一个分区内的数据是一个一个执行逻辑只有前面一个数据全部的逻辑执行完毕后，才会执行下一个数据分区内数据的执行是有序的。
    //2.不同分区数据计算是无序的。

    sc.stop()
  }

}
