package org.mkuthan.beam.examples.bigquery

import scala.annotation.unused

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.bigquery._
import org.joda.time.Instant

object SaveTypedBigQuery {
  def main(@unused cmdArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(DefaultArgs)

    val table = args.required("table")

    val records = sc
      .parallelize(1 to 100)
      .map { i =>
        BigQueryRecord.Record(
          id = i.toLong,
          timestamp = Instant.now(),
          name = s"some name $i",
          description = Some(s"some description $i"),
          attributes = List(
            BigQueryRecord.Attribute(s"key 1", s"value $i"),
            BigQueryRecord.Attribute(s"key 2", s"value $i")
          )
        )
      }

    val genericRecords = records.map(BigQueryType.toAvro[BigQueryRecord.Record])

    genericRecords.saveAsBigQueryTable(
      table = Table.Spec(table),
      schema = BigQueryType.schemaOf[BigQueryRecord.Record],
      writeDisposition = WRITE_APPEND,
      createDisposition = CREATE_NEVER
    )

    sc.run().waitUntilDone()
    ()
  }
}
