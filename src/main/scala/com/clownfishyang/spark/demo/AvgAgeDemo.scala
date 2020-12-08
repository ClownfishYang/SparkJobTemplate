package com.clownfishyang.spark.demo

import java.io.{File, PrintWriter}

import com.clownfishyang.spark.Job
import com.clownfishyang.spark.demo.WordCountDemo.readDataFile

import scala.io.Source
import scala.util.Random

/**
  * 平均年龄<br>
  *
  * 需要统计一个 1000 万人口的所有人的平均年龄。<br>
  * 假设这些年龄信息都存储在一个文件里，并且该文件的格式如下，第一列是 ID，第二列是年龄。
  * @author ClownfishYang<br>
  *         created on 2020/11/17 14:25<br>
  */
object AvgAgeDemo extends Demo {
  /**
    *
    * 功能描述:
    * 执行方法
    *
    * @return
    * @author ClownfishYang
    *         created on 2020/11/13 11:25
    */
  override def run = {
//        randomData
    val ageRDD = sc.sparkContext.parallelize(readDataFile, 5)
      .map(_.split(","))
      .filter(_.length == 2)

    // totalAge: Infinity
    // totalAge: 2955154.0
//    val totalAge = ageRDD.map(_(1)).reduce(_ + _).toDouble
    /*val totalAge = ageRDD.map(_(1).toLong).reduce(_ + _).toDouble
    val count = ageRDD.count.toDouble
    val avgAge = totalAge / count
    info(s"totalAge: $totalAge")
    info(s"count: $count")
    info(s"avg age : $avgAge")*/


    type scoreCount = (Double, Int)
    val avgAge = ageRDD.map(c => (c(1).toDouble, 1))
      .aggregate((0.0, 0))(
        (a: scoreCount, b: scoreCount) => (a._1 + b._1, a._2 + b._2),
        (a: scoreCount, b: scoreCount) => {
          ((a._1 + b._1) / (a._2 + b._2) , 1)
        }
      )._1
    info(s"avg age : $avgAge")
  }

  def randomData() = {
    val writer = new PrintWriter(new File("src/main/resources/data/user-age.data"))
    var a = 0
    val random = new Random()
    for (a <- 1 to 1000000)
      writer.println(s"$a," + random.nextInt(60))
    writer.close()
  }

}
