package de.tuberlin.dima.bdapro.muses.connector.arrow

import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, IntervalUnit, TimeUnit}

object POJOArrowTypeMappings {
  def getArrowType(typeName: String): ArrowType = {
    val currentType = typeName.toUpperCase
    val arrowType:ArrowType = currentType match {
      case "CHAR"	=> new ArrowType.Utf8
      case "NCHAR"	=> new ArrowType.Utf8
      case "VARCHAR" => new ArrowType.Utf8
      case "NVARCHAR" => new ArrowType.Utf8
      case "LONGVARCHAR" => new ArrowType.Utf8
      case "LONGNVARCHAR" => new ArrowType.Utf8
      case "NUMERIC" => new ArrowType.Decimal(20, 10)
      case "DECIMAL" => new ArrowType.Decimal(20, 10)
      case "BIT" => new ArrowType.Bool
      case "TINYINT" => new ArrowType.Int(8, true)
      case "SMALLINT" => new ArrowType.Int(16, true)
      case "INT" | "INTEGER" => new ArrowType.Int(32, true)
      case "BIGINT" => new ArrowType.Int(64, true)
      case "REAL" => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
      case "FLOAT" => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
      case "DOUBLE" => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
      case "BINARY" => new ArrowType.Binary
      case "VARBINARY" => new ArrowType.Binary
      case "LONGVARBINARY" => new ArrowType.Binary
      case "DATE" => new ArrowType.Date(DateUnit.MILLISECOND)
      case "TIME" => new ArrowType.Time(TimeUnit.MILLISECOND, 32)
      case "TIMESTAMP" => new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)
      case "CLOB" => new ArrowType.Utf8
      case "BLOB" => new ArrowType.Binary
      case _  => throw new Exception(typeName + " is not supported yet.")
    }
    return arrowType
  }
}


