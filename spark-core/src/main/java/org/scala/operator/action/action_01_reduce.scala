package org.scala.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 行动算子
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object action_01_reduce {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc: SparkContext = new SparkContext(sparkConf)
    //1.reduce() 俩俩聚合
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    val res: Int = rdd.reduce(_ + _)
    println(res)
    //2.collect() 将不同分区的数据按照分区采集数据到Driver端中进行处理，形成数组
    val ints: Array[Int] = rdd.collect()
    println(ints.mkString(","))
    //3.count() 返回RDD中元素的个数
    println(rdd.count())
    //4.first() 返回RDD中的第一个元素
    println(rdd.first())
    //5.take(n) 返回一个由R
    println(rdd.take(2).mkString(","))

    //6.takeOrdered(n) 返回RDD排序后的前n个元素
    println(rdd.takeOrdered(3)(Ordering.Int.reverse).mkString(","))
    //7.aggregate() aggregateByKey 只参与分区内计算 aggregate分区内分区间都参加计算
    println(rdd.aggregate(10)(_ + _, _ + _))
    //8.fold() 40
    println(rdd.fold(10)(_ + _))
    //9.countByValue()  Map(4 -> 1, 2 -> 1, 1 -> 1, 3 -> 1)
    println(rdd.countByValue())
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4)))
    //10.countByKey()   根据Map(a -> 2, b -> 2)
    println(rdd2.countByKey())
    //10.save()

    //11.foreach()

    sc.stop()
  }

}
