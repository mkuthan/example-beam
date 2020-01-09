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

package org.mkuthan.example.lookup

import com.spotify.scio.io.PubsubIO
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.testStreamOf
import org.apache.beam.sdk.metrics.MetricName
import org.joda.time.Duration
import org.joda.time.Instant

class CoGroupByKeyLookupTest extends PipelineSpec {

  import CoGroupByKeyLookup._

  private val lookupCacheDurationInSeconds = 10

  "CoGroupByKeyLookup" should "work" in {
    JobTest[CoGroupByKeyLookup.type]
      .args(
        "--lookupSubscriptionName=lookup-subscription",
        "--inSubscriptionName=in-subscription",
        "--outTopicName=out-topic",
        s"--lookupCacheDurationInSeconds=$lookupCacheDurationInSeconds",
        "--streaming=true"
      )
      .inputStream(
        PubsubIO[Lookup]("lookup-subscription"),
        testStreamOf[Lookup]
          .addElements(Lookup("a", "foo"))
          .advanceWatermarkToInfinity()
      )
      .inputStream(
        PubsubIO[In]("in-subscription"),
        testStreamOf[In]
          .advanceProcessingTime(Duration.standardSeconds(1))
          .addElements(In("a", "zic"))
          .advanceProcessingTime(Duration.standardSeconds(lookupCacheDurationInSeconds + 1))
          .addElements(In("a", "zac"))
          .advanceWatermarkToInfinity()
      )
      .output(PubsubIO[Out]("out-topic"))(
        _ should containInAnyOrder(
          Seq(
            Out("a", "zic", Some("foo")),
            Out("a", "zac", None)
          )
        )
      )
      .counters(_ should contain(MetricName.named("com.spotify.scio.ScioMetrics", "cached") -> 1))
      .counters(_ should contain(MetricName.named("com.spotify.scio.ScioMetrics", "expired") -> 1))
      .run()
  }
}
