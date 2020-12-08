package com.clownfishyang.spark.demo

import org.apache.spark.{HashPartitioner, Partitioner}

import scala.swing.event.Key

/**
  * 搜索词TopN<br>
  *
  * 假设某搜索引擎公司要统计过去一年搜索频率最高的 K 个科技关键词或词组，为了简化问题，假设关键词组已经被整理到一个文本文件中。<br>
  * 关键词或者词组可能会出现多次，且大小写格式可能不一致。<br>
  *
  * @author ClownfishYang<br>
  *         created on 2020/11/19 14:19<br>
  */
object SearchKeyWordsDemo extends Demo{
  /**
    *
    * 功能描述:
    * 执行方法
    *
    * @return
    * @author ClownfishYang
    *         created on 2020/11/13 11:25
    */
  override def run: Unit = {
    val stopwords = Array("a","are", "to", "the","by","your","you","and","in","of","on")

    sc.sparkContext.parallelize(readDataFile, 5)
      .filter(_.length > 0)
      .map(_.toLowerCase)
      .flatMap(_.split(","))
      .flatMap(_.split(" "))
      .map(_.trim)
      .filter(keyword => keyword.length > 0 && !stopwords.contains(keyword))
      .map((_, 1))
      .reduceByKey(_+_)
      .repartitionAndSortWithinPartitions(new HashPartitioner(20))
      .mapPartitions(_.take(20))
      // count 降序
      .sortBy(_._2, false)
      .take(10)
      .foreach(x => info(x))
  }

  case class SearchKeywords(keyword: String, count: Int)

  implicit def orderingByGradeStudentScore[A <: SearchKeywords] : Ordering[A] = {
    Ordering.by(x => -x.count)
  }



}
