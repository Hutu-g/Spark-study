package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 转换算子 groupByKey
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_19_groupByKey {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 1), ("a", 6), ("d", 1), ("a", 3)))
    //相同的的key进行value分组操作
    //形成一个对偶元组类型
    //元组第一个元素为key，第二为value的集合
    val groupRdd: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    val groupr: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
    groupRdd.collect().foreach(println)
    groupr.collect().foreach(println)
    sc.stop()
  }
}
