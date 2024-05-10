package sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 *
 * @Author: hutu
 * @Date: 2024/5/8 10:05
 */
object Spark_sql_UDF {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._ //导入隐式转换 对象里的内容
    val df: DataFrame = spark.read.json("datas/user.json")

    df.createOrReplaceTempView("user")
    //给名称加个前缀
    spark.udf.register("prefixName",(name:String)=>{
      "Name:"+name
    })
    spark.sql("select age,prefixName(username) from user").show()

    //todo 关闭环境
    spark.close()


  }

}
