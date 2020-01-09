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

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.transforms.windowing.AfterPane
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration

object CoGroupByKeyLookup {

  type K = String

  case class Lookup(key: K, lookup: String)

  case class In(key: K, payload: String)

  case class Out(key: K, payload: String, lookup: Option[String])

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val lookupSubscriptionName = args.required("lookupSubscriptionName")
    val inSubscriptionName = args.required("inSubscriptionName")
    val outTopicName = args.required("outTopicName")
    val lookupCacheDurationInSeconds = args.long("lookupCacheDurationInSeconds")

    val lookup = sc
      .pubsubSubscription[Lookup](lookupSubscriptionName)
      .withGlobalWindow(options = WindowOptions(
        trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
        accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES
      ))

    val in = sc
      .pubsubSubscription[In](inSubscriptionName)
      .withGlobalWindow(options = WindowOptions(
        trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
        accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES
      ))

    val lookupByKey = lookup.map { e => e.key -> e.lookup }
    val inByKey = in.map { e => e.key -> e.payload }

    val out = inByKey
      .cogroup(lookupByKey)
      .applyPerKeyDoFn(new CoGroupByKeyCacheDoFn(Duration.standardSeconds(lookupCacheDurationInSeconds)))
      .flatMap { case (k, v) =>
        v.map { case (payload, lookup) =>
          Out(k, payload, lookup)
        }
      }

    out.saveAsPubsub(outTopicName)

    sc.run()
    ()
  }

}
