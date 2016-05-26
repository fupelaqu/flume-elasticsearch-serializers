package com.ebiznext.flume.sink.elasticsearch

import scala.util.Try

/**
  *
  * Created by smanciot on 02/05/16.
  */
package object serializer {

  /**
    * loan pattern
    */
  def using[A <: { def close(): Unit }, B](resource: A)(f: A => B): Try[B] =
    try {
      Try(f(resource))
    } finally resource.close()

}
