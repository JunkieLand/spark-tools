package it.trenzalore.utils.logging

import org.slf4j.LoggerFactory

trait Logging {

  lazy val logger = LoggerFactory.getLogger(getClass)

}
