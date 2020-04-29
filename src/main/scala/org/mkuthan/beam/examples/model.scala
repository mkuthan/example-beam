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

package org.mkuthan.beam.examples

import com.twitter.algebird.Semigroup

case class ClientId(id: String) extends AnyVal

case class Client(id: ClientId, name: String)

case class ScreenId(id: String) extends AnyVal

case class Screen(id: ScreenId, clientId: ClientId, name: String)

case class AdId(id: String) extends AnyVal

object AdAction extends Enumeration {
  type AdAction = Value
  val Click, Impression, Unknown = Value
}

case class AdEvent(id: AdId, screenId: ScreenId, action: AdAction.AdAction) {
  lazy val isImpression: Boolean = action == AdAction.Impression
  lazy val isClick: Boolean = action == AdAction.Click
}

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
