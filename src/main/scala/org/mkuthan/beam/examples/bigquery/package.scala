package org.mkuthan.beam.examples

import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.Instant

/**
  *
  * bq rm playground-272019:beam_examples.avro_example
  * bq mk playground-272019:beam_examples.avro_example src/main/resources/schema.json
  *
  */
package object bigquery {
  val DefaultArgs = Array(
    "--project=playground-272019",
    "--tempLocation=gs://playground-272019-temp",
    "--table=playground-272019:beam_examples.avro_example"
  )
}
