package com.sgr.serviceScala.impl

import com.sgr.conf.LazyLogging
import com.sgr.serviceScala.ScalaExportService
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.stereotype.Service

import java.io.File
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, HashMap}
import scala.collection.JavaConversions.mapAsScalaMap

/**
 * @author sunguorui
 * @date 2021年12月05日 3:22 下午
 */
@Service
class ScalaExportServiceImpl extends ScalaExportService with LazyLogging{
  @Autowired
  private val spark: SparkSession = null

  override def exportData(sql: String, code: String, path: String, rows: String): util.Map[String, Int] = {
    println("sql=" + sql)
    println("path=" + path)
    val df = spark.sql(sql)
    val fileRows = df.count()
    df.show(5)
    val fileNums = fileRows / rows.toInt
    import spark.implicits._
    df.map( df => df.toSeq.map(d => {
      d match {
        case null => "null"
        case _ => d.toString
      }
    }).mkString("\t")).repartition(fileNums.toInt + 1)
      .write.text(s"file:$path")
    println("文件导出成功开始进行处理")
    fixFile(path, code)
    getRes(path)
  }

  /**
   * 统计对应文件的行数
   * 并以文件名为key  行数为value 封装map返回
   * @param dir
   * @return
   */
  def getRes(dir: String): util.HashMap[String, Int] = {
    var jMap = new HashMap[String, Int]()
    val res = new File(dir)
    val fileName: Array[File] = getFiles(res)
    fileName.foreach(x => {
      val rows = scala.io.Source.fromFile(x).getLines().size
      jMap.put(x.toString, rows)
    })
    jMap.foreach(k => println("key=" + k._1 + ",value=" + k._2))
    println("文件对应的行数统计完毕")
    println(jMap.size())
    jMap
  }

  /**
   * 将文件改成对应的文件名
   * @param dir 生成数据所在的文件
   * @param code 信息资源代码
   */
  def fixFile(dir: String, code: String) = {
    val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date: Date = new Date
    val path = new File(dir)
    val fileName: Array[File] = getFiles(path)
    val newFile = path.toString
    fileName.filter(t => t.toString.endsWith(".crc")).foreach(_.delete())
    fileName.filter(t => t.toString.contains("_SUCCESS")).foreach(_.delete())
    fileName.foreach(x =>println(x.toString))

    var num = 40000

    fileName.filter(t => t.toString.endsWith(".txt"))
      .foreach(
        x => {
          val time: String = DATE_FORMAT.format(date)
          val data = time.split("-").mkString("").split(" ").mkString("").split(":").mkString("")

          x.renameTo(new File(s"$newFile/" + s"$code-" + s"$data" + "-" + num + ".bcp"))
          num = num + 1
        })
    println("对应文件名生成完毕")
  }

  /**
   * 获取指定目录下的文件
   * @param dir 目录
   * @return
   */
  def getFiles(dir: File): Array[File] = {
    dir.listFiles.filter(_.isFile) ++
      dir.listFiles.filter(_.isDirectory).flatMap(getFiles)
  }
}
