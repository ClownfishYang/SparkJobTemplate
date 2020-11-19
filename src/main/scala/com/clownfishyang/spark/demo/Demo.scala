package com.clownfishyang.spark.demo

import com.clownfishyang.spark.Job
import com.clownfishyang.spark.demo.WordCountDemo.{getClass, option}

import scala.io.Source

/**
  * @author ClownfishYang<br>
  *         created on 2020/11/17 17:47<br>
  */
trait Demo extends Job {

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

  def readDataFile = {
    val is = getClass.getResourceAsStream(option("dataFile").toString)
    Source.fromInputStream(is).getLines().toSeq
  }
}
