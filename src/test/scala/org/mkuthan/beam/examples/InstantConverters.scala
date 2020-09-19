package org.mkuthan.beam.examples

import org.joda.time.Instant
import org.joda.time.LocalTime

object InstantConverters {
  private val BaseTime = new Instant(0)

  def stringToInstant(time: String): Instant =
    LocalTime
      .parse(time)
      .toDateTime(BaseTime)
      .toInstant
}
