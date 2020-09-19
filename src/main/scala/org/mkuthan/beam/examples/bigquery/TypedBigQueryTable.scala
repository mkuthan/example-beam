package org.mkuthan.beam.examples.bigquery

import scala.annotation.unused

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.bigquery.Table
import com.spotify.scio.bigquery._

object TypedBigQueryTable {
  def main(@unused cmdArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(DefaultArgs)
    val table = args.required("table")

    BigQueryRecord.print(sc.typedBigQueryTable[BigQueryRecord.Record](Table.Spec(table)))
    sc.run().waitUntilDone()
    ()
  }
}
