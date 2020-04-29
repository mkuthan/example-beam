# Apache Beam examples

Playground for [Apache Beam](https://beam.apache.org) and [Scio](https://github.com/spotify/scio) experiments.

## Join in fixed window

Ad impressions and clicks joined in the fixed window:

* [AdCtrFixedWindowCalculator](src/main/scala/org/mkuthan/beam/examples/AdCtrFixedWindowCalculator.scala)
* [AdCtrFixedWindowCalculatorTest](src/test/scala/org/mkuthan/example/beam/AdCtrFixedWindowCalculatorTest.scala)

Pros:

* Built-in support for the fixed window.
* Built-in support for late events.

Cons:

* High latency, CTR is always emitted at the end of window.
* An incomplete CTR for events on the windows boundaries.

## Join in fixed window with "impressions repeater"

Stay tuned

## Join in global window with "impressions cache"

Stay tuned

