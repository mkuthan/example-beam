# Apache Beam examples

Playground for [Apache Beam](https://beam.apache.org) and [Scio](https://github.com/spotify/scio) experiments.

## Join in fixed window

Ad impressions and clicks joined in the fixed window:

* [AdCtrFixedWindowCalculator](src/main/scala/org/mkuthan/beam/examples/AdCtrFixedWindowCalculator.scala)
* [AdCtrFixedWindowCalculatorTest](src/test/scala/org/mkuthan/example/beam/AdCtrFixedWindowCalculatorTest.scala)

Pros:

* Built-in support for the fixed window.
* Built-in support for late Ad events.

Cons:

* High latency, CTR is always emitted at the end of window.
* An incomplete CTR for events on the windows boundaries (e.g 11:59:00 and 12:00:00).

## Join in fixed window with "repeater"

Ad events enriched by Screen event in the fixed window, Screen events are repeated to simulate longer window on the right side of the join:

* [AdEventFixedWindowWithRepeaterEnricher](src/main/scala/org/mkuthan/beam/examples/AdEventFixedWindowWithRepeaterEnricher.scala)
* [AdEventFixedWindowWithRepeaterEnricherTest](src/test/scala/org/mkuthan/beam/examples/AdEventFixedWindowWithRepeaterEnricherTest.scala)

Pros:

* Built-in support for the fixed window.
* Built-in support for late Ad and Screen events (TODO: tests).
* Window duration for Screen events is longer than for Ad Events events effectively (Screen events are repeated).

Cons:
* Medium latency, enriched Ad Event is always emitted at the end of window.

## Join in global window with "cache"

Stay tuned

