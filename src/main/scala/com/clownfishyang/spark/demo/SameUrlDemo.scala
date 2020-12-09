package com.clownfishyang.spark.demo

import java.io.{File, PrintWriter}
import java.util.regex.Pattern
import java.util.{Random, UUID}

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileSplit, TextInputFormat}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.{HadoopRDD, RDD}

import scala.reflect.io.Directory

/**
  * 找出相同的URL<br>
  *
  * 给定 a、b 两个文件，各存放 50 亿个 URL，每个 URL 各占 64B，内存限制是 4G。请找出 a、b 两个文件共同的 URL。
  *
  * @author ClownfishYang<br>
  *         created on 2020/12/8 15:43<br>
  */
object SameUrlDemo extends Demo {
  /**
    *
    * 功能描述:
    *
    * 解答思路
    * 每个 URL 占 64B，那么 50 亿个 URL占用的空间大小约为 320GB。
    * 5,000,000,000 * 64B ≈ 5GB * 64 = 320GB
    *
    * 由于内存大小只有 4G，因此，我们不可能一次性把所有 URL 加载到内存中处理。对于这种类型的题目，一般采用分治策略，即：把一个文件中的 URL 按照某个特征划分为多个小文件，使得每个小文件大小不超过 4G，这样就可以把这个小文件读到内存中进行处理了。
    *
    * 思路如下：
    * 首先遍历文件 a，对遍历到的 URL 求 hash(URL) % 1000，根据计算结果把遍历到的 URL 存储到 a0, a1, a2, …, a999，这样每个大小约为 300MB。使用同样的方法遍历文件 b，把文件 b 中的 URL 分别存储到文件 b0, b1, b2, …, b999 中。这样处理过后，所有可能相同的 URL 都在对应的小文件中，即 a0 对应 b0, …, a999 对应 b999，不对应的小文件不可能有相同的 URL。那么接下来，我们只需要求出这 1000 对小文件中相同的 URL 就好了。
    *
    * 接着遍历 ai( i∈[0,999])，把 URL 存储到一个 HashSet 集合中。然后遍历 bi 中每个 URL，看在 HashSet 集合中是否存在，若存在，说明这就是共同的 URL，可以把这个 URL 保存到一个单独的文件中。
    *
    * @return
    * @author ClownfishYang
    *         created on 2020/11/13 11:25
    */
  override def run: Unit = {
    val dataFile = option("dataFile").toString
    option = option + ("dataFile1" -> dataFile.replace("*", "1"), "dataFile2" -> dataFile.replace("*", "2"))

    val outputPath = option("outputFile").toString
    val splitPath = if (outputPath.endsWith("/")) outputPath + "split" else outputPath + "/split"

    // 生辰数据文件
    //    randomData

    // 切分成小文件
//    splitSourceFile(dataFile, splitPath)


    // 依次读取文件，查询出重复的URL
    val resultPath = if (outputPath.endsWith("/")) outputPath + "sameUrl" else outputPath + "/sameUrl"
    // 删除原有结果
    deleteDir(resultPath)
    logger.info("start read split file, search same url...")
    val fs = new File(splitPath).listFiles(_.getName.startsWith("part-"))
    var allRdd: RDD[String] = sc.sparkContext.emptyRDD
    fs.foreach(path => {
      val p = path.getPath
      logger.info("read file : " + p)
      val curRdd = sc.sparkContext.textFile(p)
        .map(line => {
          val tuple = line.split(",")
          (tuple(0), tuple(1))
        })
        .aggregateByKey(new collection.mutable.HashSet[String]())(
          ((c, v) => c + v),
          ((c1, c2) => c1 ++ c2))
        // 重复数大于等于2
        .filter(_._2.size > 1)
        .map(_._1)
      allRdd = allRdd.union(curRdd)
    })
    logger.info("save search result : " + resultPath)
    // 结果保存为一个文件，更好的做法是使用磁盘merge操作
    //    allRdd.collect().foreach(println)
    allRdd.coalesce(1).saveAsTextFile(resultPath)
    logger.info("job success.")
  }

  /**
    *
    * 功能描述:
    *
    * 切分成小文件
    *
    * @param dataFile  源文件
    * @param splitPath 小文件地址
    * @author ClownfishYang
    *         created on 2020/12/9 17:01
    */
  private def splitSourceFile(dataFile: String, splitPath: String) = {
    // 删除原有小文件
    deleteDir(splitPath)
    // 切分
    logger.info("start split file...")
    val pattern = Pattern.compile("(url-)([0-9]){1}(.data)$")
    sc.sparkContext.hadoopFile(dataFile, classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
      .asInstanceOf[HadoopRDD[LongWritable, Text]]
      .mapPartitionsWithInputSplit((is, iter) => {
        val f = is.asInstanceOf[FileSplit]
        val matcher = pattern.matcher(f.getPath.toString)
        var idx = 0
        if (matcher.find) {
          idx = Integer.valueOf(matcher.group(2))
        }
        iter.map(line => (line._2.toString, idx))
      })
      .partitionBy(new HashPartitioner(100))
      .map { case (k, v) => s"$k,$v" }
      .saveAsTextFile(splitPath)
    logger.info("start split success.")
  }

  def deleteDir(path: String) = {
    val dir = new Directory(new File(path))
    if (dir.exists) {
      logger.info("delete path : " + path)
      dir.deleteRecursively()
      logger.info("delete success.")
    }
  }

  def randomData() = {
    val writer1 = new PrintWriter(new File(option("dataFile1").toString))
    val writer2 = new PrintWriter(new File(option("dataFile2").toString))
    val format = "http://%s.com"
    var i = 0
    val random = new Random()
    for (i <- 1 to 5000) {
      val url = format.format(UUID.randomUUID())
      writer1.println(url)
      writer2.println(if (random.nextInt(10) / 3 == 0) url else format.format(UUID.randomUUID))
    }
    writer1.close()
    writer2.close()
  }
}
