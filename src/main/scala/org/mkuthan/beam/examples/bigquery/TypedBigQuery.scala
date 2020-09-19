package org.mkuthan.beam.examples.bigquery

import scala.annotation.unused

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.bigquery._

object TypedBigQuery {
  def main(@unused cmdArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(DefaultArgs)
    val table = args.required("table")

    val query = s"""#standardsql
      SELECT id, timestamp, name, description, attributes
      FROM `$table`
      WHERE name LIKE '%9%'
      """.stripMargin

    BigQueryRecord.print(sc.typedBigQuery[BigQueryRecord.Record](Query(query.replace(":", "."))))
    sc.run().waitUntilDone()
    ()
  }
}
