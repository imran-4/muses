package de.tuberlin.dima.bdapro.muses.connector

import org.apache.arrow.vector.ValueVector

trait Writer {
  def write(vector: ValueVector, valueCount: Int, values: Array[Object])
}
