package org.mkuthan.beam.examples

import scala.annotation.unused
import scala.reflect.ClassTag

import com.google.api.services.bigquery.model.TableSchema
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificData
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.specific.SpecificRecord
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord

object AvroFunctions {

  def specificWriter[T <: SpecificRecord](schema: Schema): SpecificDatumWriter[T] =
    new SpecificDatumWriter[T](schema)

  def genericWriter(schema: Schema): GenericDatumWriter[GenericRecord] =
    new GenericDatumWriter(schema)

  def specificSchemaFactory[T <: SpecificRecord: ClassTag](@unused tableSchema: TableSchema): Schema =
    getSchema[T]()

  // TODO: implement
  def genericSchemaFactory(@unused tableSchema: TableSchema): Schema =
    AvroExampleRecord.SCHEMA$

  // TODO: logical types
  def specificRead[T <: SpecificRecord: ClassTag](schemaAndRecord: SchemaAndRecord): T =
    SpecificData
      .get()
      .deepCopy(getSchema[T](), schemaAndRecord.getRecord)
      .asInstanceOf[T]

  def genericRead(schemaAndRecord: SchemaAndRecord): GenericRecord =
    schemaAndRecord.getRecord

  private def getSchema[T <: SpecificRecord]()(implicit ct: ClassTag[T]): Schema =
    // discover Avro schema at runtime, it isn't serializable
    SpecificData
      .get()
      .getSchema(ct.runtimeClass)
}
