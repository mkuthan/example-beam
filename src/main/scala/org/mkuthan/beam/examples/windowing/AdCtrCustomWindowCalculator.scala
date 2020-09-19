package org.mkuthan.beam.examples.windowing

import com.spotify.scio.values.SCollection
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime
import org.apache.beam.sdk.transforms.windowing.AfterWatermark
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration

object AdCtrCustomWindowCalculator {

  val ImpressionToClickWindowDuration = Duration.standardMinutes(10)
  val ClickToImpressionWindowDuration = Duration.standardMinutes(1)

  def calculateCtrByScreen(
      events: SCollection[AdEvent],
      impressionToClickWindowDuration: Duration = ImpressionToClickWindowDuration,
      clickToImpressionWindowDuration: Duration = ClickToImpressionWindowDuration,
      allowedLateness: Duration = Duration.ZERO
  ): SCollection[(ScreenId, AdCtr)] = {
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

    val ctrsByScreen = events
      .withName(s"Apply Custom window with allowed lateness $allowedLateness")
      .withWindowFn(
        AdEventWindowFn(impressionToClickWindowDuration, clickToImpressionWindowDuration),
        options = windowOptions
      )
      .withName("Key AdEvent by AdId/ScreenId")
      .keyBy { adEvent => (adEvent.id, adEvent.screenId) }
      .withName("Prepare initial AdCtr from AdEvent")
      .mapValues { adEvent => AdCtr.fromAdEvent(adEvent) }
      .withName("Calculate capped CTR per ScreenId")
      .sumByKey(AdCtrCappedSemigroup)
      .withName("Discard AdId/ScreenId key")
      .mapKeys { case (_, screenId) => screenId }

    // ctrsByScreen.withWindow.debug(prefix = "ctr by screen with window:")
    // ctrsByScreen.withPaneInfo.withTimestamp.debug(prefix = "ctr by screen with paneinfo: ")

    ctrsByScreen
  }
}
