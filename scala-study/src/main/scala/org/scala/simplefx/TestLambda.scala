package org.scala.simplefx

/**
 * 匿名函数
 *
 * @Author: hutu
 * @Date: 2024/3/3 15:02
 */
object TestLambda {
  def main(args: Array[String]): Unit = {
    //简单调用
    val fun = (name: String) =>
      println(name)
    fun("我是匿名函数简单调用")
    //当作一个函数的参数
    //函数的类型为String => Unit，函数的输入类型和输出类型
    def f(func: String => Unit): Unit = {
      func("我是当作一个函数的参数")
    }
    f(fun)
    //等价于
    f((name: String) => {println(name)})
    /**
     * 简化
     */
    //（1 ）参数的类型可以省略 ，会根据形参进行自动的推导
    f((name) => {println(name)})
    //（2 ）类型省略之后 ，发现只有一个参数 ，则圆括号可以省略 ；其他情况 ：没有参数和参 数超过 1 的永远不能省略圆括号
    f(name => {println(name)})
    //（3 ）匿名函数如果只有一行 ，则大括号也可以省略
    f(name => println(name))
    //（4 ）如果参数只出现一次 则参数省略且后面参数可以用_代替
    f(println(_))
    //（5 ）如何可以推断出当前传入的printlin是一个函数体，而不是调用语句，可以直接省略下划线
    f(println)
    //最简写法
    def OneAndTow(fun:(Int,Int)=>Int):Int = {
      fun(1,2)
    }
    println(OneAndTow(-_+_))
    //高阶函数
    def dualEval(op:(Int,Int)=>Int,a:Int,b:Int):Int={
      op(a,b)
    }
    def add(a:Int,b:Int) = {
      a + b
    }
    println(dualEval(add,12,35))
    println(dualEval(_-_,12,35))
    //函数作为返回值的时候
    def f5(): Int => Unit = {
      def f6(a: Int): Unit = {
        println("f6调用" +a)
      }
      f6
    }
    println(f5()(25))


  }
}
