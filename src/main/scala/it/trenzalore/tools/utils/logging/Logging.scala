package it.trenzalore.tools.utils.logging

import org.slf4j.LoggerFactory

trait Logging {

  lazy val logger = LoggerFactory.getLogger(getClass)

}
