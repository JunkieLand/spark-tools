package it.trenzalore.tools

import it.trenzalore.utils.logging.Logging

object HelloWorldMain extends Logging {

  def main(args: Array[String]): Unit = {
    logger.info("HelloWorld is alive !")

    logger.info(sayHi("World"))
  }

  def sayHi(name: String) = s"Hello $name !"

}
