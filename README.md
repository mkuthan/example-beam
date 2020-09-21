# Apache Beam examples

Playground for [Apache Beam](https://beam.apache.org) and 
[Scio](https://github.com/spotify/scio) experiments,
driven by real-world use cases.

## Group in fixed window

The most simplified grouping example with built-in, well documented fixed window.
This is a good warm-up before a deep dive into more complex examples. 
It also shows many limitations which makes the fixed window not suitable for many real-world use cases, unfortunately.

### Domain

Advertisement impressions and clicks grouped in the fixed window to calculate CTR (Click Through Rate) per screen.
Please look at source code for more details:

* [AdCtrFixedWindowCalculator](src/main/scala/org/mkuthan/beam/examples/windowing/AdCtrFixedWindowCalculator.scala)
* [AdCtrFixedWindowCalculatorTest](src/test/scala/org/mkuthan/beam/examples/windowing/AdCtrFixedWindowCalculatorTest.scala)

### Pros

* Built-in Beam support for the fixed window, well-documented, with many examples.
* Built-in Beam support for handling late events.
* Calculator calculates CTR correctly for unordered events, e.g: click event before impression event.
Yep, in the distributed systems it's fully reasonable assumption that click event time is before impression event time.

### Cons

* High latency, calculator emits CTR at the end of window with the end of window time. 
CTR could be emitted immediately after click event with the click event time.
* Higher resources utilization. The window keeps all advertisement events until the end of the window.
Resources would be released if calculator emits CTR as soon as possible but not at the end of window.
* An incomplete CTR for events close to the windows boundaries.
If the click is very close to the impression but in a different window, the CTR will be incomplete due to unmatched events
(e.g impression at 11:59:00 and click at 12:00:00 for 10 minutes window).
For shorter window duration there are more unmatched events, for longer window duration the latency increases.

## Group in sliding window

The built-in solution for unmatched clicks and impressions close to the windows boundaries.
At first, looks like a "problem solved" but sliding window generates duplicates by its nature, 
and it is appropriate only for the moving average metrics (moving CTRs are totally fine in most cases).

### Domain

CTRs from different screens grouped in the sliding window to calculate CTR moving average.
Please look at source code for more details:

* [AdCtrSlidingWindowCalculator](src/main/scala/org/mkuthan/beam/examples/windowing/AdCtrSlidingWindowCalculator.scala)
* [AdCtrSlidingWindowCalculatorTest](src/test/scala/org/mkuthan/beam/examples/windowing/AdCtrSlidingWindowCalculatorTest.scala)

### Pros
* Built-in Beam support for the sliding window, well-documented, with many examples.
* Built-in Beam support for handling late events.
* Complete CTR for events close to the windows boundaries.

### Cons
* Huge overhead if the window duration is much longer than window period.
For window duration of 1 hour and 1 minute period you will get 60x more events to process.
* Sliding windows are not very convenient for downstream processing
and should be converted into FixedWindows in one of the final transformation steps.
* As long as SlidingWindow is not a BoundedWindow you cannot use PaneInfo assertions (inInTimePane, inLatePane, inFinalPane).  


## Group in custom window

The most complex grouping example using custom window. Be aware: dragons are there ...

### Domain

Advertisement impressions and clicks grouped in the custom window to calculate CTR (Click Through Rate) per Screen.
Please look at source code for more details:

* [AdCtrCustomWindowCalculator](src/main/scala/org/mkuthan/beam/examples/windowing/AdCtrCustomWindowCalculator.scala)
* [AdCtrCustomWindowCalculatorTest](src/test/scala/org/mkuthan/beam/examples/windowing/AdCtrCustomWindowCalculatorTest.scala)

### Pros

* Custom window looks like a built-in window.
* Built-in Beam support for handling late events (TODO: I always get PaneInfo with timing=ON_TIME, and I don't know why).
* Low latency, calculator emits CTR just after click event.
* Resource/cost friendly, domain event drives the length of the window, and the runner should be able to release resources early.

### Cons

* Lack of documentation. The best reference I found is [Streaming Systems](http://streamingsystems.net) book, see "Custom Windowing" section.
* Non-trivial implementation (e.g. it's quite easy to turn back event time and mislead runner watermark handling).
See [AdEventWindow](src/main/scala/org/mkuthan/beam/examples/windowing/AdEventWindow.scala) 
and [AdEventWindowFn](src/main/scala/org/mkuthan/beam/examples/AdEventWindowFn.scala).
* The custom window code seems to be hard to reuse, it's crafted for the specific scenario.
* The risk that custom window will not be fully supported by all runners.
* PaneInfo assertions do not work (TODO: investigate, why?)

## Join in the global window with SideInput

Beam supports shuffle like joins, but with very annoying limitation, 
windows of two sides of the join must be compatible. Compatible means: 

* Windows of the same type (you cannot join global and fixed window).
* Windows of the same length (you cannot join 10 minutes fixed window with 20 minutes fixed window).

Fortunately there is Beam built-in workaround to join incompatible windows - SideInput ... with its own limits. 

### Domain

Screen events enriched by publication in the global window, Publication events are broadcasted as IterableSideInput:
Please look at source code for more details:

* [ScreenGlobalWindowWithSideInputEnricher](src/main/scala/org/mkuthan/beam/examples/windowing/ScreenGlobalWindowWithSideInputEnricher.scala)
* [ScreenGlobalWindowWithSideInputEnricherTest](src/test/scala/org/mkuthan/beam/examples/windowing/ScreenGlobalWindowWithSideInputEnricherTest.scala)

### Pros

* Built-in support for side input.
* Very effective for small and slowly changing side input due to lack of shuffling.
What does mean small? The default Dataflow cache size for side input is 100MB, but it's runner dependent. 
What does mean slowly changing? Update on every 5 seconds [looks fine](https://beam.apache.org/documentation/patterns/side-inputs/).

### Cons

* Only IterableSideInput is fully reliable in practice, see comments in the example source code.
* It kills your scalability if there are too many publications, or publisher emits too frequently.
* I have a lot of open questions about triggering and watermark handling for side inputs.
Please look at TODOs in the test source code.
* When the runner releases the side input resources, that's the question? 
There are many OOM side input related questions on StackOverflow. 

## Join in fixed window with "repeater"

The Beam design pattern to mitigate the limitation of the compatible windows length.
For the left outer join with the distinct values on the right side, 
the right side of the join might be repeated to simulate longer window than left side window.

### Domain

Advertisement events enriched by screen event in the fixed window, screen events are repeated to simulate longer window on the right side of the join.
Please look at source code for more details:

* [AdEventFixedWindowWithRepeaterEnricher](src/main/scala/org/mkuthan/beam/examples/windowing/AdEventFixedWindowWithRepeaterEnricher.scala)
* [AdEventFixedWindowWithRepeaterEnricherTest](src/test/scala/org/mkuthan/beam/examples/windowing/AdEventFixedWindowWithRepeaterEnricherTest.scala)

### Pros

* Built-in support for the fixed window.
* Built-in support for late events (TODO: add tests for late events' scenario).
* Enricher produces correctness results even if screen event appears after the advertisement event.
* Window duration for screen events could be much longer than for advertisement events.
Mobile application emits screen events at t0 time, but advertisement events could be emitted much later.
Without [RepeatDoFn](src/main/scala/org/mkuthan/beam/examples/RepeatDoFn.scala) trick, 
the windows for the both sides of the join must be compatible (e.g. with the same window duration).
* Applicable only if the right side of the join could be "distinct". 
The duplicated screen event might be ignored or might replace the existing screen event in the lookup cache.
* Implementation is fully generic and reusable.

### Cons

* Medium latency, enricher always emits advertisement event at the end of window with window time.
Fortunately the window might be quite short because screen events are repeated.
* Higher resource utilization, the screen events must be cached and processed multiple times. 
Fortunately the frequency of screen events is typically much lower than advertisement events, so 
the negative performance/cost impact should be negligible.

## Join in global window with "cache"

The Beam design pattern to mitigate SideInput limitations (limited size and limited updates frequency).
Right side of the join is cached, it is fully scalable because the cache is distributed by key.

### Domain

Screen events enriched by publication event in the global window, 
publication events are cached and then expired to simulate the finite window:
Please look at source code for more details:

* [ScreenGlobalWindowWithLookupCacheEnricher](src/main/scala/org/mkuthan/beam/examples/windowing/ScreenGlobalWindowWithLookupCacheEnricher.scala)
* [ScreenGlobalWindowWithLookupCacheEnricherTest](src/test/scala/org/mkuthan/beam/examples/windowing/ScreenGlobalWindowWithLookupCacheEnricherTest.scala)

### Pros

* Unlimited scalability as long as keys are distributed evenly.
* The lowest latency, enricher emits screen event immediately if the publication has been already seen, 
or when late publication arrives.
* Resource/cost friendly, right side of the join (publications) is cached for the given (longer) period.
The left side of the join (screens, a lot of screen events) is also cached but for shorter period.
* Implementation is fully generic and reusable.

### Cons

* Applicable only for very specific use-case, it looks like full outer join, but it is not. 
Only the latest element on the right side of the join (publication) is used, earlier elements are discarded.
* The cache for the left side of the join (screen events for late publications) is unlimited.
It should be easy to limit the cache size, but with what kind of eviction policy (FIFO)?
* I'm sure, it's not the end of the list ...

## Save Specific Record to BigQuery

Example with saving Avro Specific record directly to BigQuery without intermediate and inefficient 
[TableRow](https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest/index.html?com/google/api/services/bigquery/model/TableRow.html).

* [SaveSpecificRecord](src/main/scala/org/mkuthan/beam/examples/bigquery/SaveSpecificRecord.scala)

### Pros

* The most efficient method for saving data to BigQuery (Avro instead of TableRow for FILE_LOADS)
* Domain model defined by Avro Schema (with logical types)
* Type-safe bindings with SpecificRecord

### Cons

* Use Scio saveAsCustomOutput due to lack of Scio API 

## Save Scio BigQueryType to BigQuery

Example with saving Scio BigQueryType to BigQuery without intermediate and inefficient 
[TableRow](https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest/index.html?com/google/api/services/bigquery/model/TableRow.html).

* [SaveTypedBigQuery](src/main/scala/org/mkuthan/beam/examples/bigquery/SaveTypedBigQuery.scala)

### Pros

* The most efficient method for saving data to BigQuery (Avro instead of TableRow for FILE_LOADS)
* Domain model defined by Scio BigQueryType

### Cons

* Write method (FILE_LOADS vs. STREAMING_INSERT) depends on collection type (Bounded vs. Unbounded)
* Intermediate conversion into GenericRecord

## Read from BigQuery using Google API library

Example with reading from BigQuery using Avro without intermediate and inefficient 
[TableRow](https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest/index.html?com/google/api/services/bigquery/model/TableRow.html).

* [TypedBigQuery](src/main/scala/org/mkuthan/beam/examples/bigquery/TypedBigQuery.scala)
* [TypedBigQueryTable](src/main/scala/org/mkuthan/beam/examples/bigquery/TypedBigQueryTable.scala)

## Read from BigQuery using Google Cloud library (Storage API)

Example with reading from BigQuery using bleeding edge BigQuery Storage API.

* [TypedBigQueryStorage](src/main/scala/org/mkuthan/beam/examples/bigquery/TypedBigQueryStorage.scala)
* [TypedBigQueryStorageTable](src/main/scala/org/mkuthan/beam/examples/bigquery/TypedBigQueryStorageTable.scala)
