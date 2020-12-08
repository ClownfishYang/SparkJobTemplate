package com.clownfishyang.spark.demo

import java.io.{File, PrintWriter}

import scala.util.Random

/**
  * 人口信息指标<br>
  * 需要对某个省的人口 (1 亿) 性别还有身高进行统计，需要计算出男女人数，男性中的最高和最低身高，以及女性中的最高和最低身高。<br>
  * 本案例中用到的源文件有以下格式, 三列分别是 ID，性别，身高 (cm)。
  *
  * @author ClownfishYang<br>
  *         created on 2020/11/19 14:08<br>
  */
object PeopleInfoMetricDemo extends Demo {

  override def run: Unit = {
//    randomData
    val peopleInfoRdd = sc.sparkContext.parallelize(readDataFile, 20)
      .map(_.split(","))
      .filter(_.length == 3)
      // sex - height
      .map(x => (x(1).toInt, x(2).toInt))

    // 性别: 0 未知，1 男，2 女
    // 数量、最低、最高、平均
    type metricType = (Int, Int, Int, Long)
    peopleInfoRdd.aggregateByKey((0, 0, 0, 0L))(
      (a: metricType, b: Int) => (a._1 + 1, if (a._2 == 0) b else Math.min(a._2, b), Math.max(a._3, b), a._4 + b),
      (a: metricType, b: metricType) => (a._1 + b._1, Math.min(a._2, b._2), Math.max(a._3, b._3), a._4 + b._4)
    ).collect()
    .foreach(x =>
      info(s"sex: ${x._1}, count: ${x._2._1}, min: ${x._2._2}, max: ${x._2._3}, avg:${x._2._4 / x._2._1} ")
    )
  }

  def randomData() = {
    val writer = new PrintWriter(new File("src/main/resources/data/people-info.data"))
    var a = 0
    val random = new Random()
    for (a <- 1 to 100000)
    // 身高最低100
      writer.println(s"$a,0," + (140 + random.nextInt(60)))
    for (a <- 1 to 200000)
    // 身高最低100
      writer.println(s"$a,1," + (160 + random.nextInt(40)))
    for (a <- 1 to 300000)
    // 身高最低100
      writer.println(s"$a,2," + (150 + random.nextInt(50)))
    for (a <- 1 to 400000)
    // 身高最低100
      writer.println(s"$a," + random.nextInt(3) + "," + (150 + random.nextInt(50)))
    writer.close()
  }

}
