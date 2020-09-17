package org.mkuthan.beam.examples

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.coders.Coder
import com.spotify.scio.coders.CoderMaterializer
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.joda.time.Instant

object AvroExample {

  /*
  val avroSchema = AvroExampleRecord.SCHEMA$.toString
  val tableSchema = new JsonObjectParser(new JacksonFactory)
    .parseAndClose(this.getClass.getResourceAsStream("/schema.json"), Charsets.UTF_8, classOf[TableSchema])
   */

  val minimalSpecific = AvroExampleRecord
    .newBuilder()
    .setId(1L)
    .setTimestamp(Instant.EPOCH.toDateTime)
    .setName("any name")
    .build()

  val fullSpecific = AvroExampleRecord
    .newBuilder()
    .setId(2L)
    .setTimestamp(Instant.EPOCH.toDateTime)
    .setName("any name")
    .setDescription("any description")
    .setAttributes(Map("first key" -> "first value", "second key" -> "second value").asJava)
    .build()

  val minimalGeneric: GenericRecord = new GenericData.Record(AvroExampleRecord.SCHEMA$)
  minimalGeneric.put("id", 1L)
  minimalGeneric.put("timestamp", Instant.EPOCH.toDateTime)
  minimalGeneric.put("name", "any name")

  val fullGeneric: GenericRecord = new GenericData.Record(AvroExampleRecord.SCHEMA$)
  fullGeneric.put("id", 1L)
  fullGeneric.put("timestamp", Instant.EPOCH.toDateTime)
  fullGeneric.put("name", "any name")
  fullGeneric.put("description", "any description")
  fullGeneric.put("attributes", Map("first key" -> "first value", "second key" -> "second value").asJava)

  /**
    * Usage:
    *
    * --tempLocation=gs://playground-272019-temp
    * --table=playground-272019:beam_examples.avro_example
    *
    * bq rm playground-272019:beam_examples.avro_example
    * bq mk playground-272019:beam_examples.avro_example src/main/resources/schema.json
    *
    * @param cmdArgs
    */
  def main(cmdArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdArgs)

    val table = args.required("table")

//    sc.parallelize(Seq(minimalSpecific, fullSpecific))
//      .saveAsCustomOutput("Save Specific", bqWriteSpecific[AvroExampleRecord](table))

//    sc.customInput("Read Specific", bqReadSpecific[AvroExampleRecord](table))
//      .debug()

//    sc.parallelize(Seq(minimalGeneric, fullGeneric))
//      .saveAsCustomOutput("Save Generic", bqWriteGeneric(table))

//    sc.customInput("Read Generic", bqReadGeneric(table))
//      .debug()

    sc.run().waitUntilDone()
    ()
  }

  private def bqWriteSpecific[T <: SpecificRecord: ClassTag](
      table: String
  ): PTransform[PCollection[T], WriteResult] =
    BigQueryIO
      .write[T]()
      .to(table)
      .withCreateDisposition(Write.CreateDisposition.CREATE_NEVER)
      .withAvroWriter(AvroFunctions.specificWriter[T])
      .withAvroSchemaFactory(AvroFunctions.specificSchemaFactory[T])

  private def bqWriteGeneric(
      table: String
  ): PTransform[PCollection[GenericRecord], WriteResult] =
    BigQueryIO
      .write[GenericRecord]()
      .to(table)
      .withCreateDisposition(Write.CreateDisposition.CREATE_NEVER)
      .withAvroWriter(AvroFunctions.genericWriter)
      .withAvroSchemaFactory(AvroFunctions.genericSchemaFactory)

  private def bqReadSpecific[T <: SpecificRecord: ClassTag](
      table: String
  )(implicit c: Coder[T]): PTransform[PBegin, PCollection[T]] = {
    BigQueryIO
      .read[T](AvroFunctions.specificRead[T])
      .from(table)
      .withCoder(CoderMaterializer.beamWithDefault(Coder[T]))
  }

  private def bqReadGeneric(
      table: String
  ): PTransform[PBegin, PCollection[GenericRecord]] = {
    BigQueryIO
      .read[GenericRecord](AvroFunctions.genericRead)
      .from(table)
      .withCoder(CoderMaterializer.beamWithDefault(Coder[GenericRecord]))
  }

}
