package de.tuberlin.dima.bdapro.muses.connector

import java.util.List

import org.apache.arrow.vector.ValueVector

trait Reader {
  def read(vector: ValueVector): List[Object]
}
