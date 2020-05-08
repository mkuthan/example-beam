package org.mkuthan.beam.examples

import java.util.Objects

import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.joda.time.Duration
import org.joda.time.Instant

class AdEventWindow(
    val start: Instant,
    val end: Instant,
    val screenId: ScreenId,
    val adId: AdId,
    val isClick: Boolean
) extends BoundedWindow {

  lazy val key = (screenId, adId)

  def merge(other: AdEventWindow): AdEventWindow = {
    require(key == other.key)

    val thisStart = start.getMillis
    val thisEnd = end.getMillis

    val otherStart = other.start.getMillis
    val otherEnd = other.end.getMillis

    // be aware - dragons below - I'm almost sure that there are corner-cases in the logic for calculation new window boundaries
    val newStart = math.min(thisStart, otherStart)
    val newEnd = if (isClick && other.isClick) {
      // the latest click
      math.max(thisStart, otherStart)
    } else if (isClick) {
      // click or impression, what is the latest
      math.max(thisStart, otherStart)
    } else if (other.isClick) {
      // click or impression, what is the latest
      math.max(otherStart, thisStart)
    } else {
      // the latest impression
      math.max(thisEnd, otherEnd)
    }

    new AdEventWindow(
      Instant.ofEpochMilli(newStart),
      Instant.ofEpochMilli(newEnd),
      screenId,
      adId,
      isClick || other.isClick
    )
  }

  override def maxTimestamp(): Instant = end

  override def equals(obj: Any): Boolean =
    obj match {
      case other: AdEventWindow =>
        start == other.start &&
          end == other.end &&
          screenId == other.screenId &&
          adId == other.adId &&
          isClick == other.isClick
      case _ => false
    }

  override def hashCode(): Int =
    Objects.hash(start, end, screenId, adId.id, Boolean.box(isClick))

  override def toString: String =
    s"CtrWindow{start=$start, end=$end, screenId='${screenId.id}', adId='${adId.id}', isClick='$isClick'}"

}

object AdEventWindow {
  def forImpression(e: AdEvent, timestamp: Instant, impressionToClickWindowDuration: Duration): AdEventWindow =
    new AdEventWindow(timestamp, timestamp.plus(impressionToClickWindowDuration), e.screenId, e.id, false)

  def forClick(e: AdEvent, timestamp: Instant, clickToImpressionWindowDuration: Duration): AdEventWindow =
    new AdEventWindow(timestamp, timestamp.plus(clickToImpressionWindowDuration), e.screenId, e.id, true)
}
