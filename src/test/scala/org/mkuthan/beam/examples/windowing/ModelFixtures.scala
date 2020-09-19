package org.mkuthan.beam.examples.windowing

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
