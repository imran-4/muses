package de.tuberlin.dima.bdapro.muses.connector.arrow.writer

import java.io.{File, FileOutputStream, OutputStream}
import java.nio.file.{Files, Paths}

import java.math.BigInteger
import java.math.BigDecimal

import com.sun.org.apache.xpath.internal.compiler.OpMapVector
import de.tuberlin.dima.bdapro.muses.connector._
import org.apache.arrow.memory._
import org.apache.arrow.vector.{DecimalVector, FieldVector, VectorSchemaRoot, complex}
import org.apache.arrow.vector.complex.{ListVector, StructVector}
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl
import org.apache.arrow.vector.complex.writer.VarCharWriter
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.{ArrowFileWriter, ArrowStreamWriter}

import scala.io.Source

class ArrowWriter extends Writer {

  override def write(): Unit = ???

  def writeDecimalVector(count: Int, decimalVector: DecimalVector, intValues: Array[Long], scale:Int): Unit = {
    var values:Array[BigDecimal] = new Array[BigDecimal](intValues.length)

    //write values
    var i = 0
    while ( i < count) {

      val decimal = new BigDecimal(BigInteger.valueOf(intValues(i)), scale)

      values(i) = decimal

      decimalVector.setSafe(i, decimal)

      i+= 1
    }
  }
}
