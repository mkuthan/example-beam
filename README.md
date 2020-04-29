# Apache Beam examples

Playground for [Apache Beam](https://beam.apache.org) and 
[Scio](https://github.com/spotify/scio) experiments,
driven by real-world use cases.

## Join in fixed window

Ad impressions and clicks joined in the fixed window to calculate CTR (Click Through Rate) per Screen:

* [AdCtrFixedWindowCalculator](src/main/scala/org/mkuthan/beam/examples/AdCtrFixedWindowCalculator.scala)
* [AdCtrFixedWindowCalculatorTest](src/test/scala/org/mkuthan/example/beam/AdCtrFixedWindowCalculatorTest.scala)

Pros:

* Built-in Beam support for the fixed window.
* Built-in Beam support for handling late Ad events.
* CTR is calculated correctly for unordered Ad events, e.g: when click appears before impression.
Yep, it's fully reasonable assumption in the distributed systems.

Cons:

* High latency, CTR is always emitted at the end of window. 
CTR could be emitted when matched click is observed with the click event time.
* Higher resource utilization.
Ad events are kept in the state until end of the window. 
If CTR was emitted earlier the resources would be released.
* An incomplete CTR for events close to the windows boundaries.
If the click is very close to the impression but in different windows the CTR will not be calculated correctly
(e.g impression at 11:59:00 and click at 12:00:00 for 10 minutes or 1 hour window).

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
But the frequency of Screen events are typically much lower than Ad events so 
the negative performance/cost impact should be negligible.

## Join in global window with "cache"

Stay tuned

