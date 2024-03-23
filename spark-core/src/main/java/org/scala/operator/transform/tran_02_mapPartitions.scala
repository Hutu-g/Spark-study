package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 转换算子mapPartitions
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_02_mapPartitions {

  //可以以分区为单位进行数据转换操作
  //但是会将整个分区的数据加载到内存进行引用
  //如果处理完的数据是不会被释放掉，存在对象的引用
  //在内存较小，数据量较大的场合下，容易出现内存溢出。
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc: SparkContext = new SparkContext(sparkConf)
    //1.mapPartitions:
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

    val mapRdd: RDD[Int] = rdd.mapPartitions(iter => {
      println("========")
      iter.map(_ * 2)
    })

//    val mapRdd: RDD[Int] = rdd.mapPartitions(_.map(_ * 2))
    mapRdd.collect().foreach(println)
    sc.stop()
  }

}
