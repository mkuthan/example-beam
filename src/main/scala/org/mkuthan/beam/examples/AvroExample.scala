package org.mkuthan.beam.examples

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType
import org.apache.avro.specific.SpecificRecord
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollection
import org.joda.time.Instant

object AvroExample {

  @BigQueryType.toTable
  case class Record(
      id: Long,
      timestamp: Instant,
      name: String,
      description: Option[String]
      // Unsupported type: Map[String,String]
      //attributes: Option[Map[String, String]]
  )

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
    records.saveAsCustomOutput("Save Specific", bqWrite[AvroExampleRecord](table))

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

  private def bqWrite[T <: SpecificRecord: ClassTag](
      table: String
  ): PTransform[PCollection[T], WriteResult] =
    BigQueryIO
      .write[T]()
      .to(table)
      .useAvroLogicalTypes()
      .withAvroWriter(AvroFunctions.writer[T])
      .withAvroSchemaFactory(AvroFunctions.schemaFactory[T])
      .withCreateDisposition(Write.CreateDisposition.CREATE_NEVER)
      .withWriteDisposition(Write.WriteDisposition.WRITE_TRUNCATE)
}
