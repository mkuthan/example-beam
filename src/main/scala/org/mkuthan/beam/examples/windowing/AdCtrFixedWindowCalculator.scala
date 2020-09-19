package org.mkuthan.beam.examples.windowing

import com.spotify.scio.values.SCollection
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime
import org.apache.beam.sdk.transforms.windowing.AfterWatermark
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration

object AdCtrFixedWindowCalculator {

  def calculateCtrByScreen(
      events: SCollection[AdEvent],
      windowDuration: Duration,
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
      .withName("Key AdEvent by AdId/ScreenId")
      .keyBy { adEvent => (adEvent.id, adEvent.screenId) }
      .withName("Prepare initial AdCtr from AdEvent")
      .mapValues { adEvent => AdCtr.fromAdEvent(adEvent) }
      .withName(s"Apply fixed window of $windowDuration and allowed lateness $allowedLateness")
      .withFixedWindows(duration = windowDuration, options = windowOptions)
      .withName("Calculate capped CTR per ScreenId")
      .sumByKey(AdCtrCappedSemigroup)
      .withName("Discard AdId/ScreenId key")
      .mapKeys { case (_, screenId) => screenId }

    // ctrsByScreen.withPaneInfo.withTimestamp.debug(prefix = "ctr by screen: ")

    ctrsByScreen
  }
}
