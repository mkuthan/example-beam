package org.mkuthan.beam.examples.bigquery

import scala.annotation.unused

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.TableId
import com.spotify.scio.ContextAndArgs
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.Element
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.DoFn.Setup
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.PeriodicImpulse
import org.joda.time.Duration
import org.joda.time.Instant

object PeriodicQuery {
  def main(@unused cmdArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(DefaultArgs)

    val table = args.required("table")

    val destinationDataset = "beam_examples"
    val destinationTable = "tmp_table"
    val destinationUri = "gs://playground-272019-temp/export"

    val query = s"""
      SELECT id, timestamp, name, description, attributes
      FROM `$table`
      WHERE name LIKE '%9%'
      """.stripMargin

    val periodicImpulse = PeriodicImpulse
      .create()
      .withInterval(Duration.standardMinutes(5))

    val exports = sc
      .customInput("PeriodicImpulse", periodicImpulse)
      .applyTransform(
        ParDo.of(new BigQueryExport(destinationDataset, destinationTable, destinationUri, query.replace(":", ".")))
      )
    exports.debug()

    sc.run()
    ()
  }

  class BigQueryExport(
      val destinationDataset: String,
      val destinationTable: String,
      val destinationUri: String,
      val query: String
  ) extends DoFn[Instant, String] {

    val destinationTableId: TableId = TableId.of(destinationDataset, destinationTable)

    var bigQuery: BigQuery = _
    var queryJobConfiguration: QueryJobConfiguration = _

    @Setup
    def setup(): Unit = {
      bigQuery = BigQueryOptions.getDefaultInstance.getService
      queryJobConfiguration = QueryJobConfiguration
        .newBuilder(query)
        .setDestinationTable(destinationTableId)
        .build
    }

    @ProcessElement
    def processElement(
        @Element element: Instant,
        receiver: OutputReceiver[String]
    ): Unit = {
      // query to BQ temporary table
      bigQuery.query(queryJobConfiguration)

      // extract from BQ table to GCS
      val table = bigQuery.getTable(destinationTableId)
      val extractJob = table.extract("AVRO", destinationUri)
      extractJob.waitFor()

      // delete BQ temporary table
      table.delete()

      receiver.output("FOO: " + element.toString)
    }
  }
}
