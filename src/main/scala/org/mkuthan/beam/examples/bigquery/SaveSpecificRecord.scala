package org.mkuthan.beam.examples.bigquery

import scala.annotation.unused
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.bigquery.CREATE_NEVER
import com.spotify.scio.bigquery.WRITE_APPEND
import org.apache.avro.specific.SpecificRecord
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollection
import org.joda.time.Duration
import org.joda.time.Instant
import org.mkuthan.beam.examples.AvroExampleRecord

object SaveSpecificRecord {

  case class UnboundedFileLoadsParams(triggeringFrequency: Duration, numFileShards: Int)

  case class UnboundedStreamingInsertParams(
      failedInsertRetryPolicy: InsertRetryPolicy = InsertRetryPolicy.retryTransientErrors
  )

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
    records.saveAsCustomOutput("Save using FILE_LOADS", bqWriteFileLoads[AvroExampleRecord](table))
    records.saveAsCustomOutput("Save using STREAMING_INSERTS", bqWriteStreamingInsert[AvroExampleRecord](table))

    sc.run().waitUntilDone()
    ()
  }

  private def bqWriteFileLoads[T <: SpecificRecord: ClassTag](
      table: String,
      writeDisposition: WriteDisposition = WRITE_APPEND,
      unboundedParams: Option[UnboundedFileLoadsParams] = None
  ): PTransform[PCollection[T], WriteResult] = {
    val io = BigQueryIO
      .write[T]()
      .to(table)
      .withMethod(Method.FILE_LOADS)
      .useAvroLogicalTypes()
      .withAvroWriter(AvroFunctions.writer[T])
      .withAvroSchemaFactory(AvroFunctions.schemaFactory[T])
      .withCreateDisposition(CREATE_NEVER)
      .withWriteDisposition(writeDisposition)
      .optimizedWrites()

    unboundedParams.fold(io) { up =>
      io.withTriggeringFrequency(up.triggeringFrequency)
        .withNumFileShards(up.numFileShards)
    }
  }

  private def bqWriteStreamingInsert[T <: SpecificRecord: ClassTag](
      table: String,
      writeDisposition: WriteDisposition = WRITE_APPEND,
      unboundedParams: Option[UnboundedStreamingInsertParams] = None
  ): PTransform[PCollection[T], WriteResult] = {
    val io = BigQueryIO
      .write[T]()
      .to(table)
      .withMethod(Method.STREAMING_INSERTS)
      .withExtendedErrorInfo()
      .withFormatFunction(AvroFunctions.formatFunction) // STREAMING_INSERTS doesn't support Avro writer
      .withCreateDisposition(CREATE_NEVER)
      .withWriteDisposition(writeDisposition)
      .optimizedWrites()

    unboundedParams.fold(io) { up => io.withFailedInsertRetryPolicy(up.failedInsertRetryPolicy) }
  }
}
