package de.tuberlin.dima.bdapro.muses.connector.arrow.reader

import java.util._

import de.tuberlin.dima.bdapro.muses.connector._
import org.apache.arrow.vector._

class ArrowReader extends Reader {

  override def read(vector: ValueVector): List[Object] = {
    return new ArrayList[Object]()
  }

  private def readTimeStampData(vector: TimeStampVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]

    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  private def readDecimalData(vector: DecimalVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  private def readIntData(vector: IntVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  private def readBitData(vector: BitVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  private def readIntervalYearData(vector: IntervalYearVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  private def readIntervalDayData(vector: IntervalDayVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  private def readTimeSecData(vector: TimeSecVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  private def readTimeMilliData(vector: TimeMilliVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  private def readTimeMicroData(vector: TimeMicroVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  private def readTimeNanoData(vector: TimeNanoVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  private def readDateDayData(vector: DateDayVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  private def readDateMilliData(vector: DateMilliVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  private def readSmallIntData(vector: SmallIntVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  private def readTinyIntData(vector: TinyIntVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  private def readBigIntData(vector: BigIntVector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  private def readFloatData(vector: Float4Vector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  private def readDoubleData(vector: Float8Vector): List[Object] = {
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  private def readVarBinaryData(vector: VarBinaryVector): List[Object] = {
    //val utf8Charset = Charset.forName("UTF-8")
    var i = 0
    val valueCount = vector.getValueCount
    var values: List[Object] = new ArrayList[Object]
    for (i <- 0 until valueCount) {
      values.add(vector.getObject(i))
    }
    return values
  }

  private def readVarCharData(vector: VarCharVector): List[Object] = {
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
