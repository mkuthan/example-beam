package org.mkuthan.beam.examples

import com.spotify.scio.values.SCollection
import com.spotify.scio.values.SideOutput
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.transforms.windowing.AfterPane
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner
import org.apache.beam.sdk.transforms.windowing.Window.OnTimeBehavior
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration

object ScreenGlobalWindowWithLookupCacheEnricher {

  private val DefaultPublicationTtlDuration = Duration.standardHours(1)

  private val GlobalWindowOptions = WindowOptions(
    trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
    accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
    timestampCombiner = TimestampCombiner.LATEST,
    onTimeBehavior = OnTimeBehavior.FIRE_IF_NON_EMPTY
  )

  def enrichByPublication(
      screens: SCollection[Screen],
      publications: SCollection[Publication],
      publicationTtl: Duration = DefaultPublicationTtlDuration
  ): (SCollection[(Screen, Publication)], SCollection[Screen]) = {
    val screensByPublicationId = screens
      .withName("Reset Screen into GlobalWindow")
      .withGlobalWindow(GlobalWindowOptions)
      .withName("Key Screen by PublicationId")
      .keyBy { screen => screen.publicationId }

    val publicationsByPublicationId = publications
      .withName("Reset Publication into GlobalWindow")
      .withGlobalWindow(GlobalWindowOptions)
      .withName("Key Publication by PublicationId")
      .keyBy { publication => publication.id }

    val screensAndPublication = screensByPublicationId
      .withName("CoGroup Screen with Publication")
      .cogroup(publicationsByPublicationId)
      .withName("Join Screen with Publication")
      .applyPerKeyDoFn(new LookupCacheDoFn(publicationTtl))
      .withName("Discard Screen with Publication join key")
      .values

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
