package com.clownfishyang.spark

import grizzled.slf4j.Logging
import org.apache.spark.sql.SparkSession

/**
 * Copyright (C), 2015-${YEAR}, 深圳市环球易购电子商务有限公司<br>
 * 任务
 * @author   ClownfishYang<br>
 * created on 2020-11-13 11:28:05<br>
 */
trait Job extends Logging {

  protected[spark] val sc = SparkSession.builder
    .appName(appName)
    .master(master)
    .getOrCreate

  protected[spark] var option: Map[String, Any] = _


  def appName: String = this.getClass.getSimpleName
  def master: String =
    "local[2]"


  /**
   *
   * 功能描述:
   * 解析app option
   * @param args
   * @return option map
   * @author ClownfishYang
   * created on 2020/11/13 11:26
   */
  def parseArgs(args: Array[String]): Map[String, Any] = {
    if (args == null || args.length == 0) {
      error("usage")
      sys.exit(1)
    }
    val arglist = args.toList

    def nextOption(map: Map[String, Any], list: List[String]): Map[String, Any] = {
      def isSwitch(s: String) = (s(0) == '-')

      list match {
        case Nil => map
        case "--max-size" :: value :: tail =>
          nextOption(map + ("maxsize" -> value.toInt), tail)
        case "--min-size" :: value :: tail =>
          nextOption(map + ("minsize" -> value.toInt), tail)
        case string :: opt2 :: tail if isSwitch(opt2) =>
          nextOption(map + ("infile" -> string), list.tail)
        case string :: Nil => nextOption(map + ("infile" -> string), list.tail)
        case option :: tail => error("Unknown option " + option)
          sys.exit(1)
      }
    }

    nextOption(Map(), arglist)
  }

  /**
   *
   * 功能描述:
   * 程序信息
   * @author ClownfishYang
   * created on 2020/11/13 11:26
   */
  def appInfo: Unit = {
    val scalaVersion = scala.util.Properties.scalaPropOrElse("version.number", "unknown")

    val versionInfo =
      s"""
         |---------------------------------------------------------------------------------
         | Scala version: $scalaVersion
         | Spark version: ${sc.version}
         | Spark master : ${sc.sparkContext.master}
         | App name: ${sc.sparkContext.appName}
         | Spark running locally? ${sc.sparkContext.isLocal}
         | Default parallelism: ${sc.sparkContext.defaultParallelism}
         | App option : ${option}
         |---------------------------------------------------------------------------------
         |""".stripMargin

    info(versionInfo)
  }

  /**
    *
    * 功能描述:
    * 执行方法
    * @return
    * @author ClownfishYang
    *         created on 2020/11/13 11:25
    */
  def run

  /**
    *
    * 功能描述:
    * 执行完成方法
    *
    * @return
    * @author ClownfishYang
    *         created on 2020/11/13 11:25
    */
  def finish: Unit = {
    debug(s"${appName} finish.")
  }

  def main(args: Array[String]): Unit = {
    option = parseArgs(args)
    appInfo
    run
    finish
  }

}
