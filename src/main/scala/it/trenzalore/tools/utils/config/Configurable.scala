package it.trenzalore.tools.utils.config

import com.typesafe.config.Config

trait Configurable {

  implicit val config: Config

}
