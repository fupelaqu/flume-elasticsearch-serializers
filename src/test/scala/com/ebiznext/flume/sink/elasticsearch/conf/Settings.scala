package com.ebiznext.flume.sink.elasticsearch.conf

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Try

/**
  *
  * Created by smanciot on 29/02/16.
  */
object Settings {

  lazy val config: Config = ConfigFactory.load("elasticsearch")

  object ElasticSearch{
    val Cluster = config getString "elasticsearch.cluster"
    val Debug = config getBoolean "elasticsearch.debug"
    val Embedded = {
      val e = Settings.config getString "elasticsearch.embedded"
      Try(getClass.getResource(e).getPath) getOrElse e
    }
    val plugins = (Settings.config getString "elasticsearch.plugins" split ',').toSeq
  }

}
