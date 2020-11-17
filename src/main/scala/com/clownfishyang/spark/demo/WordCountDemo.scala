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
object WordCountDemo extends Job {


  override def parseArgs(args: Array[String]): Map[String, Any] = {
    if (args == null || args.length == 0) {
      println("文件路径[--data-file]不能为空.")
      sys.exit(1)
    }
    val list = args.toList

    def nextOption(map: Map[String, Any], list: List[String]): Map[String, Any] = {
      list match {
        case Nil => map
        case "--data-file" :: value :: tail => nextOption(map + ("dataFile" -> value), tail)
        case "--output-file" :: value :: tail => nextOption(map + ("outputFile" -> value), tail)
        case option :: tail => println("Unknown option " + option)
          sys.exit(1)
      }
    }

    nextOption(Map(), list)
  }

  override def run: Unit = {

//    val rdd = sc.sparkContext.parallelize(Array("one", "two", "two", "three", "three", "three")).map(word => (word, 1))
    val rdd = sc.sparkContext.parallelize(readDataFile, 5)

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

  def readDataFile = {
    val is = getClass.getResourceAsStream(option("dataFile").toString)
    Source.fromInputStream(is).getLines().toSeq
  }
}
