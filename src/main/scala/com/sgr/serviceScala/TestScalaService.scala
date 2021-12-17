package com.sgr.serviceScala

import com.sgr.conf.LazyLogging
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

/**
 * @author sunguorui
 * @date 2021年12月03日 12:27 下午
 */
@Service
class TestScalaService extends LazyLogging{
  @Autowired
  private val sparkSession: SparkSession = null
  def getData(sql: String): Unit = {
    logger.info(sql)
    val df = sparkSession.sql(sql)
    df.show()
    logger.info(df.count().toString)
  }
}
