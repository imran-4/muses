package de.tuberlin.dima.bdapro.muses.connector.arrow.writer

import java.math.BigDecimal
import java.nio.charset.Charset

import de.tuberlin.dima.bdapro.muses.connector._
import org.apache.arrow.vector._


class ArrowWriter extends Writer {

  override def write(vector: ValueVector, valueCount: Int, values: Array[Object]): Unit = {

  }

  //NOTE: Keep in mind that there are different variants of setSafe() for each data type. may be we need to call those based on certain condition(s)

  private def writeTimeStampData(vector: TimeStampVector, valueCount: Int, values: Array[Int]): Unit = {
    var i = 0
    for (i <- 0 until valueCount) {
      vector.setSafe(i, values(i))
    }
    vector.setValueCount(valueCount)
  }

  private def writeDecimalData(vector: DecimalVector, valueCount: Int, values: Array[BigDecimal]): Unit = {
    var i = 0
    for (i <- 0 until valueCount) {
      vector.setSafe(i, values(i))
    }
    vector.setValueCount(valueCount)
  }

  private def writeIntData(vector: IntVector, valueCount: Int, values: Array[Int]): Unit = {
    var i = 0
    for (i <- 0 until valueCount) {
      vector.setSafe(i, values(i))
    }
    vector.setValueCount(valueCount)
  }

  private def writeBitData(vector: BitVector, valueCount: Int, values: Array[Boolean]): Unit = {
    var i = 0
    for (i <- 0 until valueCount) {
      vector.setSafe(i, if (values(i)) 1 else 0)
    }
    vector.setValueCount(valueCount)
  }

  private def writeIntervalYearData(vector: IntervalYearVector, valueCount: Int, values: Array[Int]): Unit = {
    var i = 0
    for (i <- 0 until valueCount) {
      vector.setSafe(i, values(i))
    }
    vector.setValueCount(valueCount)
  }

  private def writeIntervalDayData(vector: IntervalDayVector, valueCount: Int, values: Array[Int]): Unit = {
    var i = 0
    for (i <- 0 until valueCount) {
      vector.setSafe(i, values(i), 100) //third argument is milliseconds
    }
    vector.setValueCount(valueCount)
  }

  private def writeTimeSecData(vector: TimeSecVector, valueCount: Int, values: Array[Int]): Unit = {
    var i = 0
    for (i <- 0 until valueCount) {
      vector.setSafe(i, values(i))
    }
    vector.setValueCount(valueCount)
  }

  private def writeTimeMilliData(vector: TimeMilliVector, valueCount: Int, values: Array[Int]): Unit = {
    var i = 0
    for (i <- 0 until valueCount) {
      vector.setSafe(i, values(i))
    }
    vector.setValueCount(valueCount)
  }

  private def writeTimeMicroData(vector: TimeMicroVector, valueCount: Int, values: Array[Long]): Unit = {
    var i = 0
    for (i <- 0 until valueCount) {
      vector.setSafe(i, values(i))
    }
    vector.setValueCount(valueCount)
  }

  private def writeTimeNanoData(vector: TimeNanoVector, valueCount: Int, values: Array[Long]): Unit = {
    var i = 0
    for (i <- 0 until valueCount) {
      vector.setSafe(i, values(i))
    }
    vector.setValueCount(valueCount)
  }

  private def writeDateDayData(vector: DateDayVector, valueCount: Int, values: Array[Int]): Unit = {
    var i = 0
    for (i <- 0 until valueCount) {
      vector.setSafe(i, values(i))
    }
    vector.setValueCount(valueCount)
  }

  private def writeDateMilliData(vector: DateMilliVector, valueCount: Int, values: Array[Long]): Unit = {
    var i = 0
    for (i <- 0 until valueCount) {
      vector.setSafe(i, values(i))
    }
    vector.setValueCount(valueCount)
  }

  private def writeSmallIntData(vector: SmallIntVector, valueCount: Int, values: Array[Int]): Unit = {
    var i = 0
    for (i <- 0 until valueCount) {
      vector.setSafe(i, values(i))
    }
    vector.setValueCount(valueCount)
  }

  private def writeTinyIntData(vector: TinyIntVector, valueCount: Int, values: Array[Int]): Unit = {
    var i = 0
    for (i <- 0 until valueCount) {
      vector.setSafe(i, values(i))
    }
    vector.setValueCount(valueCount)
  }

  private def writeBigIntData(vector: BigIntVector, valueCount: Int, values: Array[Long]): Unit = {
    var i = 0
    for (i <- 0 until valueCount) {
      vector.setSafe(i, values(i))
    }
    vector.setValueCount(valueCount)
  }

  private def writeFloatData(vector: Float4Vector, valueCount: Int, values: Array[Float]): Unit = {
    var i = 0
    for (i <- 0 until valueCount) {
      vector.setSafe(i, values(i))
    }
    vector.setValueCount(valueCount)
  }

  private def writeDoubleData(vector: Float8Vector, valueCount: Int, values: Array[Double]): Unit = {
    var i = 0
    for (i <- 0 until valueCount) {
      vector.setSafe(i, values(i))
    }
    vector.setValueCount(valueCount)
  }

  private def writeVarBinaryData(vector: VarBinaryVector, valueCount: Int, values: Array[String]): Unit = {
    val utf8Charset = Charset.forName("UTF-8")
    var i = 0
    for (i <- 0 until valueCount) {
      vector.setSafe(i, values(i).getBytes(utf8Charset))
    }
    vector.setValueCount(valueCount)
  }

  private def writeVarCharData(vector: VarCharVector, valueCount: Int, values: Array[String]): Unit = {
    val utf8Charset = Charset.forName("UTF-8")
    var i = 0
    for (i <- 0 until valueCount) {
      vector.setSafe(i, values(i).getBytes(utf8Charset))
    }
    vector.setValueCount(valueCount)
  }

  //write also for complex data, dictionary, struct, list

  //following are not implemented yet, check if there is alternative available in the already implemented functions
  //  private def writeFixedSizeBinaryData(): Unit = {}
  //  private def writeTimeStampMicroTZData(): Unit = {}
  //  private def writeTimeStampMicroData(): Unit = {}
  //  private def writeTimeStampMilliTZData(): Unit = {}
  //  private def writeTimeStampMilliData(): Unit = {}
  //  private def writeTimeStampNanoTZData(): Unit = {}
  //  private def writeTimeStampNanoData(): Unit = {}
  //  private def writeTimeStampSecTZData(): Unit = {}
  //  private def writeTimeStampSecData(): Unit = {}
  //  private def writeUInt1Data(): Unit = {}
  //  private def writeUInt2Data(): Unit = {}
  //  private def writeUInt4Data(): Unit = {}
  //  private def writeUInt8Data(): Unit = {}
}
