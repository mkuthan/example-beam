package org.mkuthan.beam.examples

import java.util.Collection

import scala.jdk.CollectionConverters._

import com.spotify.scio.coders.CoderMaterializer
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.transforms.windowing.WindowFn
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn
import org.joda.time.Duration

class AdEventWindowFn(
    impressionToClickWindowDuration: Duration,
    clickToImpressionWindowDuration: Duration
) extends WindowFn[AdEvent, AdEventWindow] {

  override def assignWindows(c: WindowFn[AdEvent, AdEventWindow]#AssignContext): Collection[AdEventWindow] = {
    val timestamp = c.timestamp()
    val adEvent = c.element()
    val window = adEvent match {
      case e: AdEvent if e.isImpression => AdEventWindow.forImpression(e, timestamp, impressionToClickWindowDuration)
      case e: AdEvent if e.isClick      => AdEventWindow.forClick(e, timestamp, clickToImpressionWindowDuration)
    }
    Seq(window).asJavaCollection
  }

  override def mergeWindows(c: WindowFn[AdEvent, AdEventWindow]#MergeContext): Unit = {
    val windows = c.windows().asScala
    val windowsByKey = windows.groupBy(w => w.key)
    val mergedWindows = windowsByKey.view.mapValues(ws => ws.reduce { (w1, w2) => w1.merge(w2) })

    windowsByKey.foreach {
      case (k, ws) =>
        c.merge(ws.asJavaCollection, mergedWindows(k))
    }
  }

  override def isCompatible(other: WindowFn[_, _]): Boolean =
    other.isInstanceOf[AdEventWindowFn]

  override def windowCoder(): Coder[AdEventWindow] =
    CoderMaterializer.beamWithDefault(com.spotify.scio.coders.Coder[AdEventWindow])

  override def getDefaultWindowMappingFn: WindowMappingFn[AdEventWindow] =
    throw new UnsupportedOperationException()
}

object AdEventWindowFn {
  def apply(impressionToClickWindowDuration: Duration, clickToImpressionWindowDuration: Duration): AdEventWindowFn =
    new AdEventWindowFn(impressionToClickWindowDuration, clickToImpressionWindowDuration)
}
