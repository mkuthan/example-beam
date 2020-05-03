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

trait ModelFixtures {
  val anyPublicationId = PublicationId("any client id")
  val anyPublicationVersion = "any publication version"
  val anyPublication = Publication(anyPublicationId, anyPublicationVersion)

  val anyScreenId = ScreenId("any screen id")
  val anyScreen = Screen(anyScreenId, anyPublicationId)

  val anyAdId = AdId("any ad id")
  val anyAdEvent = AdEvent(anyAdId, anyScreenId, AdAction.Unknown)

  val anyClick = anyAdEvent.copy(action = AdAction.Click)
  val anyImpression = anyAdEvent.copy(action = AdAction.Impression)

  val anyAdCtr = AdCtr.unknown(anyAdId)

  val adCtrUndefined = AdCtr.click(anyAdId)
  val adCtrZero = AdCtr.impression(anyAdId)
  val adCtrOne = AdCtr(anyAdId, clicks = 1, impressions = 1)

  val adCtrUndefinedByScreen = (anyScreenId, adCtrUndefined)
  val adCtrZeroByScreen = (anyScreenId, adCtrZero)
  val adCtrOneByScreen = (anyScreenId, adCtrOne)
}
