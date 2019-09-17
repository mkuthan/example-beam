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

import com.spotify.scio._
import com.spotify.scio.coders.Coder
import com.spotify.scio.coders.CoderMaterializer
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.SideInput
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.io.GenerateSequence
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.state.StateSpecs
import org.apache.beam.sdk.state.ValueState
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.DoFn.StateId
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.windowing.AfterPane
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration

object SideInputExamples {

  type LookupMap = Map[Int, Option[String]]

  def generateLookupStream(accumulationMode: AccumulationMode = AccumulationMode.ACCUMULATING_FIRED_PANES)
    (implicit sc: ScioContext): SCollection[LookupMap] =
    sc.customInput(
      "generateLookupStream",
      GenerateSequence
        .from(0)
        .withRate(1, Duration.standardSeconds(5))
    ).withGlobalWindow(
      options = WindowOptions(
        trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
        accumulationMode = accumulationMode
      )
    ).map(i =>
      i.toInt
    ).map[LookupMap] {
      case 0 => Map(1 -> Some("a"), 2 -> Some("b"), 3 -> Some("c"))
      case 1 => Map(4 -> Some("z"))
      case 2 => Map(1 -> Some("x"))
      case 3 => Map(2 -> None)
      case _ => Map()
    }

  def generateMainStream()
    (implicit sc: ScioContext): SCollection[Int] =
    sc.customInput(
      "generateMainStream",
      GenerateSequence
        .from(0)
        .withRate(1, Duration.standardSeconds(1))
    ).withGlobalWindow(
      options = WindowOptions(
        trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
        accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES
      )
    ).map(i =>
      i.toInt
    )

  def joinMainStreamWithSideInput(mainStream: SCollection[Int], sideInput: SideInput[_]): SCollection[String] =
    mainStream
      .withSideInputs(sideInput)
      .map {
        case (elem, side) =>
          val lookup = side(sideInput)
          s"$elem: $lookup"
      }.toSCollection
}

object SideInputAsMultiMapExample {

  import SideInputExamples._

  def main(cmdlineArgs: Array[String]): Unit = {
    implicit val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.optionsAs[StreamingOptions].setStreaming(true)

    val lookupStream = generateLookupStream()
    val mainStream = generateMainStream()

    val lookupSideInput = lookupStream.flatMap(_.seq).asMultiMapSideInput

    val joinedStream = joinMainStreamWithSideInput(mainStream, lookupSideInput)
    joinedStream.debug(prefix = "joinedStream: ")

    sc.run()
  }
}

object SideInputWithStatefulDoFnExample {

  import SideInputExamples._

  def main(cmdlineArgs: Array[String]): Unit = {
    implicit val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.optionsAs[StreamingOptions].setStreaming(true)

    val lookupStream = generateLookupStream(AccumulationMode.DISCARDING_FIRED_PANES)
    val mainStream = generateMainStream()

    val lookupStreamByFakeKey = lookupStream
      .map(lookupMap => KV.of[String, LookupMap]("any key", lookupMap))

    val lookupSideInput = lookupStreamByFakeKey.applyTransform(ParDo.of(new CacheDoFn))
      .flatMap(kv => kv.getValue.seq)
      .asMapSideInput

    val joinedStream = joinMainStreamWithSideInput(mainStream, lookupSideInput)
    joinedStream.debug(prefix = "joinedStream: ")

    sc.run()
  }

  type CacheDoFnType = DoFn[KV[String, LookupMap], KV[String, LookupMap]]

  class CacheDoFn extends CacheDoFnType {
    @StateId("cache") private val cacheSpecs = StateSpecs.value[LookupMap](
      CoderMaterializer.beamWithDefault(Coder[LookupMap]))

    @ProcessElement
    def processElement(
        context: CacheDoFnType#ProcessContext,
        @StateId("cache") cacheState: ValueState[LookupMap]
    ): Unit = {
      val lookupMap = context.element().getValue

      val lookupMapCache = cacheState.read()
      val finalLookupMap = lookupMapCache ++ lookupMap

      cacheState.write(finalLookupMap)
      context.output(KV.of(context.element().getKey, finalLookupMap))
    }
  }

}

object SideInputAsMapExample {

  //  Exception in thread "main" org.apache.beam.sdk.Pipeline$PipelineExecutionException: java.lang.IllegalArgumentException: Duplicate values for 1
  //  at org.apache.beam.runners.direct.DirectRunner$DirectPipelineResult.waitUntilFinish(DirectRunner.java:348)
  //  at org.apache.beam.runners.direct.DirectRunner$DirectPipelineResult.waitUntilFinish(DirectRunner.java:318)
  //  at org.apache.beam.runners.direct.DirectRunner.run(DirectRunner.java:213)
  //  at org.apache.beam.runners.direct.DirectRunner.run(DirectRunner.java:67)
  //  at org.apache.beam.sdk.Pipeline.run(Pipeline.java:315)
  //  at org.apache.beam.sdk.Pipeline.run(Pipeline.java:301)
  //  at com.spotify.scio.ScioContext.execute(ScioContext.scala:587)
  //  at com.spotify.scio.ScioContext$$anonfun$run$1.apply(ScioContext.scala:575)
  //  at com.spotify.scio.ScioContext$$anonfun$run$1.apply(ScioContext.scala:563)
  //  at com.spotify.scio.ScioContext.requireNotClosed(ScioContext.scala:683)
  //  at com.spotify.scio.ScioContext.run(ScioContext.scala:563)
  //  at org.mkuthan.example.SideInputAsMapExample$.main(sideInputExamples.scala:96)
  //  at org.mkuthan.example.SideInputAsMapExample.main(sideInputExamples.scala)
  //  Caused by: java.lang.IllegalArgumentException: Duplicate values for 1
  //  at org.apache.beam.sdk.values.PCollectionViews$MapViewFn.apply(PCollectionViews.java:326)
  //  at org.apache.beam.sdk.values.PCollectionViews$MapViewFn.apply(PCollectionViews.java:311)
  //  at org.apache.beam.runners.direct.SideInputContainer$SideInputContainerSideInputReader.get(SideInputContainer.java:261)
  //  at org.apache.beam.repackaged.direct_java.runners.core.SimpleDoFnRunner.sideInput(SimpleDoFnRunner.java:243)
  //  at org.apache.beam.repackaged.direct_java.runners.core.SimpleDoFnRunner.access$900(SimpleDoFnRunner.java:74)
  //  at org.apache.beam.repackaged.direct_java.runners.core.SimpleDoFnRunner$DoFnProcessContext.sideInput(SimpleDoFnRunner.java:532)
  //  at com.spotify.scio.values.MapSideInput.get(SideInput.scala:114)
  //  at com.spotify.scio.values.MapSideInput.get(SideInput.scala:111)
  //  at com.spotify.scio.values.SideInput.getCache(SideInput.scala:45)
  //  at com.spotify.scio.values.SideInput.getCache$(SideInput.scala:41)
  //  at com.spotify.scio.values.MapSideInput.getCache(SideInput.scala:111)
  //  at com.spotify.scio.values.SideInputContext.apply(SideInput.scala:130)
  //  at org.mkuthan.example.SideInputExamples$.$anonfun$joinMainStreamWithSideInput$1(sideInputExamples.scala:75)
  //  at org.mkuthan.example.SideInputExamples$.$anonfun$joinMainStreamWithSideInput$1$adapted(sideInputExamples.scala:73)
  //  at com.spotify.scio.util.FunctionsWithSideInput$$anon$3.processElement(FunctionsWithSideInput.scala:58)

  import SideInputExamples._

  def main(cmdlineArgs: Array[String]): Unit = {
    implicit val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.optionsAs[StreamingOptions].setStreaming(true)

    val lookupStream = generateLookupStream()
    val mainStream = generateMainStream()

    val lookupSideInput = lookupStream.flatMap(_.seq).asMapSideInput

    val joinedStream = joinMainStreamWithSideInput(mainStream, lookupSideInput)
    joinedStream.debug(prefix = "joinedStream: ")

    sc.run()
  }
}

object SideInputAsSingletonExample {

  //  Exception in thread "main" org.apache.beam.sdk.Pipeline$PipelineExecutionException: java.lang.IllegalArgumentException: PCollection with more than one element accessed as a singleton view. Consider using Combine.globally().asSingleton() to combine the PCollection into a single value
  //  at org.apache.beam.runners.direct.DirectRunner$DirectPipelineResult.waitUntilFinish(DirectRunner.java:348)
  //  at org.apache.beam.runners.direct.DirectRunner$DirectPipelineResult.waitUntilFinish(DirectRunner.java:318)
  //  at org.apache.beam.runners.direct.DirectRunner.run(DirectRunner.java:213)
  //  at org.apache.beam.runners.direct.DirectRunner.run(DirectRunner.java:67)
  //  at org.apache.beam.sdk.Pipeline.run(Pipeline.java:315)
  //  at org.apache.beam.sdk.Pipeline.run(Pipeline.java:301)
  //  at com.spotify.scio.ScioContext.execute(ScioContext.scala:587)
  //  at com.spotify.scio.ScioContext$$anonfun$run$1.apply(ScioContext.scala:575)
  //  at com.spotify.scio.ScioContext$$anonfun$run$1.apply(ScioContext.scala:563)
  //  at com.spotify.scio.ScioContext.requireNotClosed(ScioContext.scala:683)
  //  at com.spotify.scio.ScioContext.run(ScioContext.scala:563)
  //  at org.mkuthan.example.SideInputAsSingletonExample$.main(sideInputExamples.scala:167)
  //  at org.mkuthan.example.SideInputAsSingletonExample.main(sideInputExamples.scala)
  //  Caused by: java.lang.IllegalArgumentException: PCollection with more than one element accessed as a singleton view. Consider using Combine.globally().asSingleton() to combine the PCollection into a single value
  //  at org.apache.beam.sdk.transforms.View$SingletonCombineFn.apply(View.java:358)
  //  at org.apache.beam.sdk.transforms.Combine$BinaryCombineFn.addInput(Combine.java:521)
  //  at org.apache.beam.sdk.transforms.Combine$BinaryCombineFn.addInput(Combine.java:489)
  //  at org.apache.beam.sdk.transforms.Combine$CombineFn.apply(Combine.java:445)
  //  at org.apache.beam.sdk.transforms.Combine$GroupedValues$1.processElement(Combine.java:2167)

  import SideInputExamples._

  def main(cmdlineArgs: Array[String]): Unit = {
    implicit val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.optionsAs[StreamingOptions].setStreaming(true)

    val lookupStream = generateLookupStream()
    val mainStream = generateMainStream()

    val lookupSideInput = lookupStream.asSingletonSideInput

    val joinedStream = joinMainStreamWithSideInput(mainStream, lookupSideInput)
    joinedStream.debug(prefix = "joinedStream: ")

    sc.run()
  }
}
