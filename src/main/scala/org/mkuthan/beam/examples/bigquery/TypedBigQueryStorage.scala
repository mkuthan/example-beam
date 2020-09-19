package org.mkuthan.beam.examples.bigquery

import scala.annotation.unused

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.bigquery.Table
import com.spotify.scio.bigquery._

object TypedBigQueryStorage {
  def main(@unused cmdArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(DefaultArgs)
    val table = args.required("table")

    val rowRestriction = "name LIKE '%9%'"

    BigQueryRecord.print(sc.typedBigQueryStorage[BigQueryRecord.Record](Table.Spec(table), rowRestriction))
    sc.run().waitUntilDone()
    ()
  }
}
