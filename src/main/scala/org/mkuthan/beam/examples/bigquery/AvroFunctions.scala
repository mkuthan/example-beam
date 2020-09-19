package org.mkuthan.beam.examples.bigquery

import scala.annotation.unused
import scala.reflect.ClassTag

import com.google.api.services.bigquery.model.TableSchema
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificData
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.specific.SpecificRecord

object AvroFunctions {

  def writer[T <: SpecificRecord](schema: Schema): SpecificDatumWriter[T] =
    new SpecificDatumWriter[T](schema)

  def schemaFactory[T <: SpecificRecord](@unused tableSchema: TableSchema)(implicit ct: ClassTag[T]): Schema = {
    SpecificData
      .get()
      .getSchema(ct.runtimeClass)
  }
}
