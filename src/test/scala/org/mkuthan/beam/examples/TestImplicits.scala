package org.mkuthan.beam.examples

import org.apache.beam.sdk.testing.TestStream
import org.apache.beam.sdk.values.TimestampedValue

object TestImplicits {

  import InstantConverters._

  implicit class TestStreamBuilderOps[T](builder: TestStream.Builder[T]) {

    def advanceWatermarkTo(time: String): TestStream.Builder[T] =
      builder.advanceWatermarkTo(stringToInstant(time))

    def addElementsAtTime(time: String, elements: T*): TestStream.Builder[T] = {
      val timestampedElements = elements.map { element => TimestampedValue.of(element, stringToInstant(time)) }
      builder.addElements(timestampedElements.head, timestampedElements.tail: _*)
    }
  }

}
