package com.sgr.conf

import org.slf4j.{Logger, LoggerFactory}

/**
 * @author sunguorui
 * @date 2021年12月17日 10:42 上午
 */
trait LazyLogging {

  protected lazy val logger: Logger = {
    LoggerFactory.getLogger("spring-spark")
  }

}
