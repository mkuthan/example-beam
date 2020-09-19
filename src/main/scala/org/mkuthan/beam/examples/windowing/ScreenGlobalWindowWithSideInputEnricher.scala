package org.mkuthan.beam.examples.windowing

import com.spotify.scio.values.SCollection
import com.spotify.scio.values.SideOutput
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.transforms.windowing.AfterPane
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode

object ScreenGlobalWindowWithSideInputEnricher {

  private val GlobalWindowOptions = WindowOptions(
    trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
    accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
    timestampCombiner = TimestampCombiner.LATEST
  )

  def enrichByPublication(
      screens: SCollection[Screen],
      publications: SCollection[Publication]
  ): (SCollection[(Screen, Publication)], SCollection[Screen]) = {
    val screenInGlobalWindow = screens
      .withName("Reset Screen into GlobalWindow")
      .withGlobalWindow(GlobalWindowOptions)

    val publicationsInGlobalWindow = publications
      .withName("Reset Publication into GlobalWindow")
      .withGlobalWindow(GlobalWindowOptions)

    // IterableSideInput is the only working side input, keep event time to find out the latest publication
    val publicationsSideInput = publicationsInGlobalWindow.withTimestamp.asIterableSideInput

    //
    // For SingletonSideInput with "element count at *least*" trigger you will get sooner or later:
    //
    //  Exception in thread "main" org.apache.beam.sdk.Pipeline$PipelineExecutionException: java.lang.IllegalArgumentException: PCollection with more than one element accessed as a singleton view. Consider using Combine.globally().asSingleton() to combine the PCollection into a single value
    //  at org.apache.beam.runners.direct.DirectRunner$DirectPipelineResult.waitUntilFinish(DirectRunner.java:348)
    //  at org.apache.beam.runners.direct.DirectRunner$DirectPipelineResult.waitUntilFinish(DirectRunner.java:318)
    //  at org.apache.beam.runners.direct.DirectRunner.run(DirectRunner.java:213)
    //  at org.apache.beam.runners.direct.DirectRunner.run(DirectRunner.java:67)
    //  at org.apache.beam.sdk.Pipeline.run(Pipeline.java:315)
    //  at org.apache.beam.sdk.Pipeline.run(Pipeline.java:301)

    //
    // For MapSideInput keyed by publicationId, when you update any publication using existing key you will get:
    //
    // Exception in thread "main" org.apache.beam.sdk.Pipeline$PipelineExecutionException: java.lang.IllegalArgumentException: Duplicate values for 1
    //  at org.apache.beam.runners.direct.DirectRunner$DirectPipelineResult.waitUntilFinish(DirectRunner.java:348)
    //  at org.apache.beam.runners.direct.DirectRunner$DirectPipelineResult.waitUntilFinish(DirectRunner.java:318)
    //  at org.apache.beam.runners.direct.DirectRunner.run(DirectRunner.java:213)
    //  at org.apache.beam.runners.direct.DirectRunner.run(DirectRunner.java:67)
    //  at org.apache.beam.sdk.Pipeline.run(Pipeline.java:315)
    //  at org.apache.beam.sdk.Pipeline.run(Pipeline.java:301)

    val screensAndPublication = screenInGlobalWindow
      .withSideInputs(publicationsSideInput)
      .withName("Join Screen with Publication using IterableSideInput")
      .map {
        case (screen, ctx) =>
          val publications = ctx(publicationsSideInput).filter {
            case (publication, _) => publication.id == screen.publicationId
          }.toSeq

          val latestPublication = publications
            .sortWith { case ((_, ts1), (_, ts2)) => ts1.isBefore(ts2) }
            .lastOption
            .map { case (publication, _) => publication }

          (screen, latestPublication)
      }
      .withName("Discard side input")
      .toSCollection

    val screensWithoutPublication = SideOutput[Screen]()

    val (screensEnriched, sideOutputs) = screensAndPublication
      .withSideOutputs(screensWithoutPublication)
      .withName("Discard Screen without Publication")
      .flatMap {
        case (adEventAndScreen, ctx) =>
          adEventAndScreen match {
            case (screen, Some(publication)) => Some((screen, publication))
            case (screen, None)              => ctx.output(screensWithoutPublication, screen); None
          }
      }

    (screensEnriched, sideOutputs(screensWithoutPublication))
  }
}
