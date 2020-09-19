package org.mkuthan.beam.examples.bigquery

import scala.annotation.unused
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.bigquery.CREATE_NEVER
import com.spotify.scio.bigquery.WRITE_TRUNCATE
import org.apache.avro.specific.SpecificRecord
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollection
import org.joda.time.Instant
import org.mkuthan.beam.examples.AvroExampleRecord

object SaveAvroToBigQuery {
  def main(@unused cmdArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(DefaultArgs)

    val table = args.required("table")

    val records = sc
      .parallelize(1 to 100)
      .map { i =>
        AvroExampleRecord
          .newBuilder()
          .setId(i.toLong)
          .setTimestamp(Instant.now().toDateTime)
          .setName(s"some name $i")
          .setDescription(s"some description $i")
          .setAttributes(
            Map(
              s"key 1" -> s"value $i",
              s"key 2" -> s"value $i"
            ).asJava
          )
          .build()
      }
    records.saveAsCustomOutput("Save Specific File Loads", bqWrite[AvroExampleRecord](table))

    // Writing avro formatted data is only supported for FILE_LOADS, however the method was STREAMING_INSERT
    // TODO: STREAMING_INSERTS with retrying

    sc.run().waitUntilDone()
    ()
  }
  private def bqWrite[T <: SpecificRecord: ClassTag](
      table: String
  ): PTransform[PCollection[T], WriteResult] =
    BigQueryIO
      .write[T]()
      .to(table)
      // TODO: handle unbounded SCollection (TriggeringFrequency, NumFileShards are required)
      .withMethod(Method.FILE_LOADS)
      .useAvroLogicalTypes()
      .withAvroWriter(AvroFunctions.writer[T])
      .withAvroSchemaFactory(AvroFunctions.schemaFactory[T])
      .withCreateDisposition(CREATE_NEVER)
      .withWriteDisposition(WRITE_TRUNCATE)
}
