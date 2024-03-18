package org.scala.simplefx

/**
 *  函数至简原则
 * @Author: hutu
 * @Date: 2024/3/3 12:43
 */
object Simplify {
  def main(args: Array[String]): Unit = {
    //1.return 可以省略 2.如果函数体只有一行代码，可以省略花括号
    def f1(name:String):String = name
    //3.返回值类型如果能够推断出来，那么可以省略（:和返回值类型一起省略）
    def f2(name:String) = name
    //4.如果有 return，则不能省略返回值类型，必须指定
    def f3(name:String):String = {
      return name
    }
    //5如果函数明确声明 unit，那么即使函数体中使用 return 关键字也不起作用返回的是空
    def f4(name: String): Unit = {
      return name
    }
    //6.Scala 如果期望是无返回值类型，可以省略等号
    def f5(name: String){
      println(name)
    }
    //7.如果函数无参，但是声明了参数列表，那么调用时，小括号，可加可不加
    def f6() {
      println("我是f6")
    }
    f6
    //8.如果函数没有参数列表，那么小括号可以省略，调用时小括号必须省略
    def f7 {
      println("我是f7")
    }
    f7
    //9.匿名函数  lambda表达式
    (name: String) => {println(name)}

    println(f1("hahha"))
  }

}
