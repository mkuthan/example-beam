package org.mkuthan.beam.examples.bigquery

import scala.annotation.unused

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.ExtractJobConfiguration
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.TableId
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.bigquery.BigQueryType
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.AvroIO
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.Element
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.DoFn.Setup
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.PeriodicImpulse
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.transforms.Watch
import org.joda.time.Duration
import org.joda.time.Instant

object PeriodicQuery {
  def main(@unused cmdArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(DefaultArgs)

    val table = args.required("table")

    val tmpProject = "playground-272019"
    val tmpDataset = "beam_examples"
    val tmpTable = "tmp_table"

    val tmpGcsUri = "gs://playground-272019-temp/export/export-*.avro"

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
        ParDo.of(new BigQueryExport(tmpProject, tmpDataset, tmpTable, tmpGcsUri, query.replace(":", ".")))
      )
    exports.debug(prefix = "Export: ")

    val reader = FileIO
      .`match`()
      .filepattern(tmpGcsUri)
      .continuously(Duration.standardMinutes(1), Watch.Growth.never())

    val parseGenericRecord = new SerializableFunction[GenericRecord, BigQueryRecord.Record] {
      override def apply(input: GenericRecord): BigQueryRecord.Record =
        BigQueryType.fromAvro[BigQueryRecord.Record](input)
    }

    val load = sc
      .customInput("Watch for new Files", reader)
      .applyTransform("Read watched File", FileIO.readMatches())
      .applyTransform("Foo", AvroIO.parseFilesGenericRecords(parseGenericRecord))
    load.debug(prefix = "Load: ")

    sc.run()
    ()
  }

  class BigQueryExport(
      val tmpProject: String,
      val tmpDataset: String,
      val tmpTable: String,
      val tmpGcsUri: String,
      val query: String
  ) extends DoFn[Instant, String] {

    val tmpTableId: TableId = TableId.of(tmpProject, tmpDataset, tmpTable)

    var bigQuery: BigQuery = _

    var queryJobConfiguration: QueryJobConfiguration = _
    var extractJobConfiguration: ExtractJobConfiguration = _

    @Setup
    def setup(): Unit = {
      bigQuery = BigQueryOptions.getDefaultInstance.getService

      queryJobConfiguration = QueryJobConfiguration
        .newBuilder(query)
        .setDestinationTable(tmpTableId)
        .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
        .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
        .build

      extractJobConfiguration = ExtractJobConfiguration
        .newBuilder(tmpTableId, tmpGcsUri)
        .setCompression("SNAPPY")
        .setFormat("AVRO")
        .setUseAvroLogicalTypes(true)
        .build();
    }

    @ProcessElement
    def processElement(
        @Element element: Instant,
        receiver: OutputReceiver[String]
    ): Unit = {
      // query to BQ temporary table
      val queryJob = bigQuery.create(JobInfo.of(queryJobConfiguration))
      val completedQueryJob = queryJob.waitFor()
      println(completedQueryJob)

      // extract BQ temporary table to GCS
      val extractJob = bigQuery.create(JobInfo.of(extractJobConfiguration))
      val completedExtractJob = extractJob.waitFor()
      println(completedExtractJob)

      receiver.output("FOO: " + element.toString)
    }
  }
}
