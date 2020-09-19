package org.mkuthan.beam.examples

import scala.reflect.ClassTag

import cats.kernel.Eq
import com.spotify.scio.coders.Coder
import com.spotify.scio.testing.SCollectionMatchers
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.joda.time.Instant
import org.scalatest.matchers.Matcher

trait TimestampedMatchers {
  this: SCollectionMatchers =>

  import InstantConverters._

  def inOnTimePane[T: ClassTag](begin: String, end: String)(matcher: MatcherBuilder[T]): Matcher[T] =
    inOnTimePane(new IntervalWindow(stringToInstant(begin), stringToInstant(end)))(matcher)

  def inFinalPane[T: ClassTag](begin: String, end: String)(matcher: MatcherBuilder[T]): Matcher[T] =
    inFinalPane(new IntervalWindow(stringToInstant(begin), stringToInstant(end)))(matcher)

  def inLatePane[T: ClassTag](begin: String, end: String)(matcher: MatcherBuilder[T]): Matcher[T] =
    inLatePane(new IntervalWindow(stringToInstant(begin), stringToInstant(end)))(matcher)

  def containValueAtTime[T: Coder: Eq](
      time: String,
      value: T
  ): IterableMatcher[SCollection[(T, Instant)], (T, Instant)] =
    containValue((value, stringToInstant(time)))

  def containSingleValueAtTime[T: Coder: Eq](
      time: String,
      value: T
  ): SingleMatcher[SCollection[(T, Instant)], (T, Instant)] =
    containSingleValue((value, stringToInstant(time)))

  def containInAnyOrderAtTime[T: Coder: Eq](
      value: Iterable[(String, T)]
  ): IterableMatcher[SCollection[(T, Instant)], (T, Instant)] =
    containInAnyOrder(value.map { case (time, v) => (v, stringToInstant(time)) })

  def containValueAtWindowTime[T: Coder: Eq](
      time: String,
      value: T
  ): IterableMatcher[SCollection[(T, Instant)], (T, Instant)] =
    containValue((value, stringToInstant(time).minus(1)))

  def containSingleValueAtWindowTime[T: Coder: Eq](
      time: String,
      value: T
  ): SingleMatcher[SCollection[(T, Instant)], (T, Instant)] =
    containSingleValue((value, stringToInstant(time).minus(1)))

  def containInAnyOrderAtWindowTime[T: Coder: Eq](
      value: Iterable[(String, T)]
  ): IterableMatcher[SCollection[(T, Instant)], (T, Instant)] =
    containInAnyOrder(value.map { case (time, v) => (v, stringToInstant(time).minus(1)) })

}
