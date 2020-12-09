package com.clownfishyang.spark.demo

import com.clownfishyang.spark.Job

import scala.io.Source
import scala.util.parsing.json.JSON

/**
  * WordCount
  *
  * @author ClownfishYang<br>
  *         created on 2020/11/13 10:52<br>
  */
object WordCountDemo extends Demo {

  override def run: Unit = {

//    val rdd = sc.sparkContext.parallelize(Array("one", "two", "two", "three", "three", "three")).map(word => (word, 1))
    val rdd = sc.sparkContext.parallelize(readDataFile(), 5)

    // 去除结尾逗号
    def dropComma(s: String) = if (s.endsWith(",")) s.dropRight(1) else s

    type wordCount = (String, Int)
    rdd.map(dropComma)
      // 解析json
      .map(JSON.parseFull(_).get.asInstanceOf[Map[String, Any]]).map(x => x("title").asInstanceOf[String])
      // 分词
      .flatMap(_.split(" "))
      .map((_, 1))
      // 计数
      .reduceByKey(_ + _)
//      .groupByKey().map(x => (x._1, x._2.sum))
//      .aggregateByKey(0)(((a, v) => a + v), ((a, v) => a + v))
//      .foldByKey(0)(_ + _)
//      .combineByKey((i: Int) => i, (a: Int,b: Int) => a + b, (c: Int, d: Int) => c + d)
      .collect()
      .foreach(println)
  }

}
