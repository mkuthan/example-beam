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

import com.spotify.scio.ContextAndArgs
import org.apache.beam.sdk.options.StreamingOptions
import org.joda.time.Instant

object TimestampAttributeExamples {

  case class Event(id: String, ts: Instant) {
    def toAttributes(): Map[String, String] = Map(Event.IdAttribute -> id, Event.TimestampAttribute -> ts.toString())
  }

  object Event {
    val IdAttribute = "id"
    val TimestampAttribute = "ts"
  }

  val EventsSubscriptionConf = "eventSubscription"
  val OutputTopicConf = "outputTopics"

  def main(cmdlineArgs: Array[String]): Unit = {
    implicit val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.optionsAs[StreamingOptions].setStreaming(true)


    val eventsSubscription = args.required(EventsSubscriptionConf)
    val outputTopic = args.required(OutputTopicConf)

    val events = sc.pubsubSubscription[Event](
      eventsSubscription,
      idAttribute = Event.IdAttribute, timestampAttribute = Event.TimestampAttribute)

    val output = events.map { event =>
      (Event(event.id, event.ts), event.toAttributes())
    }

    output.saveAsPubsub(outputTopic, idAttribute = Event.IdAttribute, timestampAttribute = Event.TimestampAttribute)

    sc.run()
  }
}
