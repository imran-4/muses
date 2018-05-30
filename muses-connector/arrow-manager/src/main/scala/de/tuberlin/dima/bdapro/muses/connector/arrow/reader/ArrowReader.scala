package de.tuberlin.dima.bdapro.muses.connector.arrow.reader

import de.tuberlin.dima.bdapro.muses.connector._
import org.apache.arrow.vector.DecimalVector

class ArrowReader extends Reader {
  override def read(): Unit = ???

  def readDecimalVector(count: Int, decimalVector: DecimalVector  ): Unit = {
    var i = 0
    decimalVector.setValueCount(count)
    while ( i < count) {
      val value = decimalVector.getObject(i)
      println("###")
      println("index: " + i)
      println("value: " + value)
      i += 1
    }
  }

}
