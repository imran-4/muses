package de.tuberlin.dima.bdapro.muses.connector.arrow.reader

import java.util._

import de.tuberlin.dima.bdapro.muses.connector._
import org.apache.arrow.vector._

class ArrowReader {

//  override def read(vector: ValueVector): List[Object] = {
//
//    //do casting/uncasting here...
//
//    var values: List[Object] = readData(vector)
//    return values
//  }

  def readTimeStampData(vector: TimeStampVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]

    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  def readDecimalData(vector: DecimalVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  def readIntData(vector: IntVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  def readBitData(vector: BitVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  def readIntervalYearData(vector: IntervalYearVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  def readIntervalDayData(vector: IntervalDayVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  def readTimeSecData(vector: TimeSecVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  def readTimeMilliData(vector: TimeMilliVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  def readTimeMicroData(vector: TimeMicroVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  def readTimeNanoData(vector: TimeNanoVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  def readDateDayData(vector: DateDayVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  def readDateMilliData(vector: DateMilliVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  def readSmallIntData(vector: SmallIntVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  def readTinyIntData(vector: TinyIntVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  def readBigIntData(vector: BigIntVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  def readFloatData(vector: Float4Vector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  def readDoubleData(vector: Float8Vector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  def readVarBinaryData(vector: VarBinaryVector): List[Object] = {
    //val utf8Charset = Charset.forName("UTF-8")
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  def readVarCharData(vector: VarCharVector): List[Object] = {
    //val utf8Charset = Charset.forName("UTF-8")
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }
}