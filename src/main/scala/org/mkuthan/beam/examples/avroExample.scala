package org.mkuthan.beam.examples

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType
import org.apache.avro.specific.SpecificRecord
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollection
import org.joda.time.Instant

/**
  * --project=playground-272019
  * --tempLocation=gs://playground-272019-temp
  * --table=playground-272019:beam_examples.avro_example
  *
  * bq rm playground-272019:beam_examples.avro_example
  * bq mk playground-272019:beam_examples.avro_example src/main/resources/schema.json
  *
  */
object AvroWriterExample {

  def main(cmdArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdArgs)

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
              s"key $i A" -> s"value $i A",
              s"key $i B" -> s"value $i B"
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

object AvroReaderExample {
  @BigQueryType.toTable
  case class Record(
      id: Long,
      timestamp: Instant,
      name: String,
      description: Option[String]
      // attributes: Unsupported type: Map[String,String]
  )

  def main(cmdArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdArgs)

    val table = args.required("table")

    sc.typedBigQueryTable[Record](Table.Spec(table))
      .count
      .debug(prefix = "Table: ")

    sc.typedBigQueryStorage[Record](Table.Spec(table))
      .count
      .debug(prefix = "Table (via Storage API): ")

    val query = s"""
      SELECT id, timestamp, name, description 
      FROM `$table` 
      WHERE name LIKE '%9%'
      """.stripMargin

    sc.typedBigQuery[Record](Query(query.replace(":", ".")))
      .count
      .debug(prefix = "Query: ")

    val rowRestriction = "name LIKE '%9%'"

    sc.typedBigQueryStorage[Record](Table.Spec(table), rowRestriction)
      .count
      .debug(prefix = "Query (via Storage API): ")

    sc.run().waitUntilDone()
    ()
  }
}
