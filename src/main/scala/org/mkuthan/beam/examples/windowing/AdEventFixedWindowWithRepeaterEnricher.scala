package org.mkuthan.beam.examples.windowing

import com.spotify.scio.values.SCollection
import com.spotify.scio.values.SideOutput
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime
import org.apache.beam.sdk.transforms.windowing.AfterWatermark
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration

object AdEventFixedWindowWithRepeaterEnricher {

  val DefaultFixedWindowDuration = Duration.standardMinutes(10)
  val DefaultScreenTtlDuration = Duration.standardMinutes(10)

  def enrichByScreen(
      events: SCollection[AdEvent],
      screens: SCollection[Screen],
      windowDuration: Duration = DefaultFixedWindowDuration,
      screenTtl: Duration = DefaultScreenTtlDuration,
      allowedLateness: Duration = Duration.ZERO
  ): (SCollection[(AdEvent, Screen)], SCollection[AdEvent]) = {
    val windowOptions = WindowOptions(
      allowedLateness = allowedLateness,
      trigger = Repeatedly.forever(
        AfterWatermark
          .pastEndOfWindow()
          .withLateFirings(
            AfterProcessingTime.pastFirstElementInPane()
          )
      ),
      accumulationMode = AccumulationMode.ACCUMULATING_FIRED_PANES
    )

    val eventsByScreenId = events
      .withName("Key AdEvent by ScreenId")
      .keyBy { adEvent => adEvent.screenId }
      .withName(s"Apply fixed window on AdEvent of $windowDuration and allowed lateness $allowedLateness")
      .withFixedWindows(duration = windowDuration, options = windowOptions)

    val screenByScreenId = screens
      .withName("Key Screen by ScreenId")
      .keyBy { screen => screen.id }
      .withName(s"Repeat Screen on every $windowDuration for $screenTtl")
      .applyPerKeyDoFn(new RepeatDoFn(windowDuration, screenTtl))
      .withName(s"Apply fixed window on Screen of $windowDuration and allowed lateness $allowedLateness")
      .withFixedWindows(duration = windowDuration, options = windowOptions)

    val eventsAndScreen = eventsByScreenId
      .withName("Join AdEvent with Screen")
      .leftOuterJoin(screenByScreenId)
      .withName("Discard ScreenId join key")
      .values

    val eventsWithoutScreen = SideOutput[AdEvent]()

    val (eventsEnriched, sideOutputs) = eventsAndScreen
      .withSideOutputs(eventsWithoutScreen)
      .withName("Discard AdEvent without Screen")
      .flatMap {
        case (adEventAndScreen, ctx) =>
          adEventAndScreen match {
            case (adEvent, Some(screen)) => Some((adEvent, screen))
            case (adEvent, None)         => ctx.output(eventsWithoutScreen, adEvent); None
          }
      }

    (eventsEnriched, sideOutputs(eventsWithoutScreen))
  }
}
