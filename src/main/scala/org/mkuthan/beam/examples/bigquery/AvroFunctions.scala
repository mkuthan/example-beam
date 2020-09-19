package org.mkuthan.beam.examples.bigquery

import scala.annotation.unused
import scala.reflect.ClassTag

import com.google.api.services.bigquery.model.TableRow
import com.google.api.services.bigquery.model.TableSchema
import com.spotify.scio.extra.bigquery.AvroConverters
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificData
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.specific.SpecificRecord

object AvroFunctions {

  def writer[T <: SpecificRecord](schema: Schema): SpecificDatumWriter[T] =
    new SpecificDatumWriter[T](schema)

  def schemaFactory[T <: SpecificRecord: ClassTag](@unused tableSchema: TableSchema): Schema =
    getSchema[T]

  def formatFunction[T <: SpecificRecord](record: T): TableRow =
    AvroConverters.toTableRow(record)

  private def getSchema[T <: SpecificRecord](implicit ct: ClassTag[T]): Schema =
    SpecificData
      .get()
      .getSchema(ct.runtimeClass)
}
