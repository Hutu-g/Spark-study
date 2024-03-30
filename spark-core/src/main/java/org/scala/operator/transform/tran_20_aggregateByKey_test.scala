package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 转换算子 aggregateByKey
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_20_aggregateByKey_test {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("a", 6), ("b", 1), ("a", 3), ("b", 4)), 2)
    //获取相同key的平均值
    //第一个0表示相同key的value，对比的初始值，第二个0表示相同key出现的次数
    val aggRdd: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (t, v) => {
        (t._1 + v, t._2 + 1)
      }, (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      })
    val res: RDD[(String, Int)] = aggRdd.mapValues { case (num, count) =>
      num / count
    }
    res.collect().foreach(println)
    sc.stop()
  }
}
