package com.sgr.serviceScala

/**
 * @author sunguorui
 * @date 2021年12月05日 3:20 下午
 */
trait ScalaExportService {

  def exportData(sql: String, code: String, path: String, rows: String): java.util.Map[String, Int]

}
