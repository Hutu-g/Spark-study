package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 *
 * @Author: hutu
 * @Date: 2024/5/8 10:05
 */
object Spark_sql_UDAF {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate() //导入隐式转换 对象里的内容
    val df: DataFrame = spark.read.json("datas/user.json")

    df.createOrReplaceTempView("user")
    //给名称加个前缀
    spark.udf.register("ageAvg",new MyAvgUDAF)
    spark.sql("select ageAvg(age) from user").show()

    //todo 关闭环境
    spark.close()


  }

  /**
   * 自定义聚合两数类:计算年龄的平均值
   */
  class MyAvgUDAF extends  UserDefinedAggregateFunction{
    override def inputSchema: StructType = {
      //输入数据的结构
      StructType(
          Array(
          StructField("age",LongType)
        )
      )
    }
    //缓冲区数据结构
    override def bufferSchema: StructType = {
      StructType(
        Array(
          StructField("total", LongType),
          StructField("count", LongType)
        )
      )
    }
    //输出数据结构类型
    override def dataType: DataType = LongType
    //稳定性
    override def deterministic: Boolean = true
    //缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
//      buffer(0) = 0L
//      buffer(1) = 0L
      buffer.update(0,0L)
      buffer.update(1,0L)
    }
    //根据输入的值更新缓冲区
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0,buffer.getLong(0)+ input.getLong(0))
      buffer.update(1,buffer.getLong(1)+1)
    }
    //缓冲区数据合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0,buffer1.getLong(0) + buffer2.getLong(0))
      buffer1.update(1,buffer1.getLong(1) + buffer2.getLong(1))
    }
    //计算
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0)/buffer.getLong(1)
    }
  }

}
