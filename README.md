# Apache Beam examples

Playground for [Apache Beam](https://beam.apache.org) and 
[Scio](https://github.com/spotify/scio) experiments,
driven by real-world use cases.

## Group in fixed window

Ad impressions and clicks grouped in the fixed window to calculate CTR (Click Through Rate) per Screen:

* [AdCtrFixedWindowCalculator](src/main/scala/org/mkuthan/beam/examples/AdCtrFixedWindowCalculator.scala)
* [AdCtrFixedWindowCalculatorTest](src/test/scala/org/mkuthan/example/beam/AdCtrFixedWindowCalculatorTest.scala)

Pros:

* Built-in Beam support for the fixed window.
* Built-in Beam support for handling late events.
* CTR is calculated correctly for unordered events, e.g: when click appears before impression.
Yep, it's fully reasonable assumption in the distributed systems.

Cons:

* High latency, CTR is always emitted at the end of window. 
CTR could be emitted immediately when matched click is seen with the click event time.
* Higher resource utilization. Events are kept in the state until end of the window.
If CTR was emitted earlier the resources would be released.
* An incomplete CTR for events close to the windows boundaries.
If the click is very close to the impression but in a different window the CTR will not be calculated correctly
(e.g impression at 11:59:00 and click at 12:00:00 for 10 minutes window).

## Group in sliding window

CTRs from different screens grouped in the sliding window to calculate CTR moving average:

* [AdCtrSlidingWindowCalculator](src/main/scala/org/mkuthan/beam/examples/AdCtrSlidingWindowCalculator.scala)
* [AdCtrSlidingWindowCalculatorTest](src/test/scala/org/mkuthan/beam/examples/AdCtrSlidingWindowCalculatorTest.scala)

Pros:
* Built-in Beam support for the sliding window.
* Built-in Beam support for handling late events.

Cons:
* Huge overhead if the window duration is much longer than window period.
* Sliding windows are not very convenient for downstream processing and should be converted into FixedWindows in one of the final transformation steps.
It also allows writing test with window boundaries verification (SlidingWindow is not a BoundedWindow).  

## Join in fixed window with "repeater"

Ad events enriched by Screen event in the fixed window, Screen events are repeated to simulate longer window on the right side of the join:

* [AdEventFixedWindowWithRepeaterEnricher](src/main/scala/org/mkuthan/beam/examples/AdEventFixedWindowWithRepeaterEnricher.scala)
* [AdEventFixedWindowWithRepeaterEnricherTest](src/test/scala/org/mkuthan/beam/examples/AdEventFixedWindowWithRepeaterEnricherTest.scala)

Pros:

* Built-in support for the fixed window.
* Built-in support for late Ad and Screen events (TODO: tests).
* Ad event is enriched correctly even if Screen event appears after the Ad event.
* Window duration for Screen events could be much longer than for Ad Events events.
Without [RepeatDoFn](src/main/scala/org/mkuthan/beam/examples/RepeatDoFn.scala) trick, 
the windows for the both sides of the join must be compatible (e.g. with the same window duration).

Cons:

* Medium latency, enriched Ad Event is always emitted at the end of window.
But the window might be quite short because Screen events are repeated.
* Higher resource utilization, the Screen events must be cached. 
But the frequency of Screen events are typically much lower than Ad events, so 
the negative performance/cost impact should be negligible.

Alternatives:

* Fixed window with longer duration (driven by required screen TTL). 

## Join in global window with SideInput

Screen events enriched by Publication in global window, Publication events are broadcasted as IterableSideInput:

* [ScreenGlobalWindowWithSideInputEnricher](src/main/scala/org/mkuthan/beam/examples/ScreenGlobalWindowWithSideInputEnricher.scala)
* [ScreenGlobalWindowWithSideInputEnricherTest](src/test/scala/org/mkuthan/beam/examples/ScreenGlobalWindowWithSideInputEnricherTest.scala)

Pros:

* Built-in support for side input.
* Effective for small and slowly changing side input.

Cons:

* Only IterabelSideInput is reliable in practice, see comments in the above example.
* It kills your scalability if there are too many publications, or the publications will be emmited too frequently.   

## Join in global window with "cache"

Screen events enriched by Publication event in global window, Publication events are cached and expired to simulate finite window:

* [ScreenGlobalWindowWithLookupCacheEnricher](src/main/scala/org/mkuthan/beam/examples/ScreenGlobalWindowWithLookupCacheEnricher.scala)
* [ScreenGlobalWindowWithLookupCacheEnricherTest](src/test/scala/org/mkuthan/beam/examples/ScreenGlobalWindowWithLookupCacheEnricherTest.scala)

Pros:

* The lowest latency, Screen event is emitted immediately if the publication has been already seen, 
or when late publication is processed.
* Resource/cost friendly, only right side of the join (publications) are cached for given period of time.

Cons:

* Applicable only for very specific use-case, it looks like full outer join but it is not. 
Only the latest element in the right side of the join (Publication) is used, earlier elements are discarded.
