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
package org.mkuthan.beam

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

  def containSingleValueAtTime[T: Coder: Eq](
      time: String,
      value: T
  ): SingleMatcher[SCollection[(T, Instant)], (T, Instant)] =
    containSingleValue((value, stringToInstant(time)))

  def containInAnyOrderAtTime[T: Coder: Eq](
      value: Iterable[(String, T)]
  ): IterableMatcher[SCollection[(T, Instant)], (T, Instant)] =
    containInAnyOrder(value.map { case (time, v) => (v, stringToInstant(time)) })

  def containSingleValueAtWindowTime[T: Coder: Eq](
      time: String,
      value: T
  ): SingleMatcher[SCollection[(T, Instant)], (T, Instant)] =
    containSingleValue((value, stringToInstant(time).minus(1)))
}
