package com.clownfishyang.scala.demo

import java.sql.Statement
import java.util.regex.Pattern

/**
  * Copyright (C), 2015-2020<br>
  * $END$<br>
  *
  * @author ClownfishYang<br>
  *         created on 2020/11/27 10:10<br>
  */
object FunctionDemo {
  def main(args: Array[String]): Unit = {

    val pattern = Pattern.compile("(url-)([0-9]){1}(.data)$")
//    val v = "asdfsafd/url-1"
//    val v = "asdfsafd/2.data"
    val v = "asdfsafd/url-.data"
//    val v = "asdfsafd/url-1.data"
    val matcher = pattern.matcher(v)
    if (matcher.find) {
      println(matcher.group)
    }


//    var print = (i: Any) => println(i)
//    Array(1,2,3).foreach(print)

//    def add(x: Int, y: Int, f: Int => Int) = f(x) + f(y)
//    println(add(-5,6,Math.abs))

    /*Statement s = null;

    def add10(x:Int):Int = x + 10
    def mult5(x:Int):Int = x * 5
    def mult5AfterAdd10(x:Int) = mult5(add10(x))*/
  }


}
