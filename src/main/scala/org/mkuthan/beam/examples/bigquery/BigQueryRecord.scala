package org.mkuthan.beam.examples.bigquery

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection
import org.joda.time.Instant

object BigQueryRecord {
  @BigQueryType.toTable
  case class Record(
      id: Long,
      timestamp: Instant,
      name: String,
      description: Option[String],
      attributes: List[Attribute]
  )
  case class Attribute(key: String, value: String)

  def print(records: SCollection[Record]) = {
    records.take(1).debug(prefix = "Sample: ")
    records.count.debug(prefix = "Count: ")
  }
}
