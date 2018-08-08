package de.tuberlin.dima.bdapro.muses.connector.arrow

import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, IntervalUnit, TimeUnit}

object POJOArrowTypeMappings {

  def getArrowType(typeName: String): ArrowType = {
    val currentType = typeName.toUpperCase
    val arrowType:ArrowType = currentType match {
      case "INT" | "INTEGER" => new ArrowType.Int(32, true)
      case "LONG" | "BIGINT"   => new ArrowType.Int(64, true)
      case "UINT" | "UNSIGNEDINT"  => new ArrowType.Int(32, false)
      case "SHORT" => new ArrowType.Int(16, true)
      case "USHORT" | "UNSIGNEDSHORT" => new ArrowType.Int(16, true)
      case "DOUBLE" => new ArrowType.Decimal(20, 3)
      case "DATE"  => new ArrowType.Date(DateUnit.DAY)
      case "VARCHAR" | "CHAR"  => new ArrowType.Utf8()
      case "FLOAT"  => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
      case "DECIMAL" | "NUMERIC"  => new ArrowType.Decimal(30, 3)
      case "BOOLEAN" | "BOOL" => new ArrowType.Bool
      case "NULL" | "NONE"  => new ArrowType.Null
      case "STRUCT" | "CLASS" | "DICTIONARY" => new ArrowType.Struct
      //not sure about type names (on the left side) used for sources
      case "BINARY"  => new ArrowType.Binary
      case "FIXEDSIZEBINARY"  => new ArrowType.FixedSizeBinary(8)
      case "FIXEDSIZELIST"  => new ArrowType.FixedSizeList(32)
      case "INTERVAL"  => new ArrowType.Interval(IntervalUnit.DAY_TIME)
      case "LIST"  => new ArrowType.List
      case "TIME"  => new ArrowType.Time(TimeUnit.MILLISECOND, 1)
      case "TIMESTAMP"  => new ArrowType.Timestamp(TimeUnit.MILLISECOND, "CEST")
      case _  => throw new Exception(typeName + " is not supported yet.") // the default, catch-all
    }
    return arrowType
  }
}