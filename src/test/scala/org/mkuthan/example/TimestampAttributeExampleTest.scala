// Copyright (C) 2011-2012 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.mkuthan.example

import scala.concurrent.duration._

import com.spotify.scio.io.PubsubIO
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.testStreamOf
import org.apache.beam.sdk.values.TimestampedValue
import org.joda.time.{Duration => JDuration}
import org.joda.time.{Instant => JInstant}

class TimestampAttributeExampleTest extends PipelineSpec {

  import TimestampAttributeExamples._

  private val eventsSubscription = "events-subscription"
  private val outputTopic = "output-topic"

  private val baseTime = new JInstant(0)

  private val anyEvent = Event("id1", baseTime)

  "Timestamp" should "be propagated from subscription to output topic" in {

    val event1 = Event("id1", at(10 second))
    val event2 = Event("id2", at(20 seconds))

    JobTest[TimestampAttributeExamples.type]
      .args(
        s"--$EventsSubscriptionConf=$eventsSubscription",
        s"--$OutputTopicConf=$outputTopic")
      .inputStream(
        PubsubIO.readCoder[Event](
          eventsSubscription,
          idAttribute = Event.IdAttribute,
          timestampAttribute = Event.TimestampAttribute),
        testStreamOf[Event]
          .advanceWatermarkTo(baseTime)
          .addElements(event1, event2)
          .advanceWatermarkToInfinity())
      .output(
        PubsubIO.withAttributes[Event](
          outputTopic,
          idAttribute = Event.IdAttribute,
          timestampAttribute = Event.TimestampAttribute)) { events =>
        events should containInAnyOrder(Seq(
          (event1, event1.toAttributes()),
          (event2, event2.toAttributes()),
        ))
      }
      .run()
  }

  private def at(duration: Duration): JInstant =
    baseTime.plus(JDuration.millis(duration.toMillis))

  private def eventAt(e: Event, duration: Duration): TimestampedValue[Event] =
    TimestampedValue.of(e, at(duration))
}
