package org.mkuthan.beam.examples.windowing

import com.spotify.scio.values.SCollection
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime
import org.apache.beam.sdk.transforms.windowing.AfterWatermark
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration

object AdCtrSlidingWindowCalculator {

  def calculateCtr(
      ctrsByScreen: SCollection[(ScreenId, AdCtr)],
      windowDuration: Duration,
      windowPeriod: Duration,
      allowedLateness: Duration = Duration.ZERO
  ): SCollection[AdCtr] = {
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

    val ctrs = ctrsByScreen
      .withName(s"Apply sliding window of $windowDuration, period: $windowPeriod and allowed lateness $allowedLateness")
      .withSlidingWindows(windowDuration, windowPeriod, options = windowOptions)
      .withName("Calculate total CTR")
      .sumByKey(AdCtrTotalSemigroup)
      .withName("Discard ScreenId key")
      .values
      .withName(s"Reset sliding window into fixed window of $windowPeriod")
      .withFixedWindows(windowPeriod)

    // ctrs.withPaneInfo.withTimestamp.debug(prefix = "ctr: ")

    ctrs
  }

}
