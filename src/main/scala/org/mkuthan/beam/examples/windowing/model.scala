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

package org.mkuthan.beam.examples.windowing

import com.twitter.algebird.Semigroup

case class PublicationId(id: String) extends AnyVal
case class ScreenId(id: String) extends AnyVal
case class AdId(id: String) extends AnyVal

/**
  * Screen publication, defines all screen details known when the screen is published.
  * Publication is emitted when screen editor changes what should be presented on the Screen.
  *
  * @param id unique publication identifier
  * @param version publication version
  */
case class Publication(id: PublicationId, version: String)

/**
  * Screen is emitted by mobile application when new screen is presented.
  * Many Screen events are typically emitted during client journey in the mobile application, join with Publication for publication details.
  *
  * @param id unique screen identifier
  * @param publicationId screen publication identifier
  */
case class Screen(id: ScreenId, publicationId: PublicationId)

/**
  * Ad event action type.
  */
object AdAction extends Enumeration {
  type AdAction = Value
  val Click, Impression, Unknown = Value
}

/**
  * Ad event is emitted for every client interaction with Ad presented on the mobile application screens.
  * Many Ad events are typically emitted on single screen, join with Screen for screen details.
  *
  * @param id unique Ad identifier
  * @param screenId screen identifier
  * @param action client action
  */
case class AdEvent(id: AdId, screenId: ScreenId, action: AdAction.AdAction) {
  lazy val isImpression: Boolean = action == AdAction.Impression
  lazy val isClick: Boolean = action == AdAction.Click
}

/**
  * Ad CTR (Click Through Rate) metric.
  *
  * @param id unique Ad identifier
  * @param clicks number of Ad click events
  * @param impressions number of Ad impression events
  */
case class AdCtr(id: AdId, clicks: Int, impressions: Int)

object AdCtr {

  def fromAdEvent(adEvent: AdEvent): AdCtr = adEvent.action match {
    case AdAction.Click      => AdCtr.click(adEvent.id)
    case AdAction.Impression => AdCtr.impression(adEvent.id)
    case _                   => AdCtr.unknown(adEvent.id)
  }

  def click(id: AdId): AdCtr = new AdCtr(id, clicks = 1, impressions = 0)

  def impression(id: AdId): AdCtr = new AdCtr(id, clicks = 0, impressions = 1)

  def unknown(id: AdId): AdCtr = new AdCtr(id, clicks = 0, impressions = 0)
}

object AdCtrCappedSemigroup extends Semigroup[AdCtr] {
  override def plus(adCtr1: AdCtr, adCtr2: AdCtr): AdCtr = {
    require(adCtr1.id == adCtr2.id)

    AdCtr(
      adCtr1.id,
      Math.min(1, adCtr1.clicks + adCtr2.clicks),
      Math.min(1, adCtr1.impressions + adCtr2.impressions)
    )
  }
}

object AdCtrTotalSemigroup extends Semigroup[AdCtr] {
  override def plus(adCtr1: AdCtr, adCtr2: AdCtr): AdCtr = {
    require(adCtr1.id == adCtr2.id)

    AdCtr(
      adCtr1.id,
      adCtr1.clicks + adCtr2.clicks,
      adCtr1.impressions + adCtr2.impressions
    )
  }
}
