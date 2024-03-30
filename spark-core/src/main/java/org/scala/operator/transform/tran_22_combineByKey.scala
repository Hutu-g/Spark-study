package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 转换算子 combineByKey
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_22_combineByKey {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("a", 6), ("b", 1), ("a", 3), ("b", 4)), 2)
    //第一个参数：将相同key的第一个数据进行结构的转换
    //第二个参数：分区内计算规则
    //第三个参数：分区间计算规则
    val aggRdd: RDD[(String, (Int, Int))] = rdd.combineByKey(
      v=>(v,1),
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      })
    val res: RDD[(String, Int)] = aggRdd.mapValues { case (num, count) =>
      num / count
    }
    res.collect().foreach(println)
    sc.stop()
  }
}
