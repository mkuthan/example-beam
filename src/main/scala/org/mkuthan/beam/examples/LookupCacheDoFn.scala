package org.mkuthan.beam.examples

import scala.jdk.CollectionConverters._

import com.spotify.scio.coders.Coder
import com.spotify.scio.coders.CoderMaterializer
import com.typesafe.scalalogging.LazyLogging
import org.apache.beam.sdk.state.BagState
import org.apache.beam.sdk.state.CombiningState
import org.apache.beam.sdk.state.StateSpecs
import org.apache.beam.sdk.state.TimeDomain
import org.apache.beam.sdk.state.Timer
import org.apache.beam.sdk.state.TimerSpecs
import org.apache.beam.sdk.state.ValueState
import org.apache.beam.sdk.transforms.Combine
import org.apache.beam.sdk.transforms.Combine.Holder
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.AlwaysFetched
import org.apache.beam.sdk.transforms.DoFn.Element
import org.apache.beam.sdk.transforms.DoFn.OnTimer
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.DoFn.StateId
import org.apache.beam.sdk.transforms.DoFn.TimerId
import org.apache.beam.sdk.transforms.DoFn.Timestamp
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.values.KV
import org.joda.time.Duration
import org.joda.time.Instant
import org.mkuthan.beam.examples.LookupCacheDoFn.LookupCacheDoFnType

object LookupCacheDoFn {
  type InputType[K, V, Lookup] = KV[K, (Iterable[V], Iterable[Lookup])]
  type OutputType[K, V, Lookup] = KV[K, (V, Option[Lookup])]

  type LookupCacheDoFnType[K, V, Lookup] = DoFn[InputType[K, V, Lookup], OutputType[K, V, Lookup]]

  type LookupStateType[Lookup] = ValueState[(Instant, Lookup)]
  type MaxTimestampStateType = CombiningState[Instant, Combine.Holder[Instant], Instant]

  final val KeyCacheKey = "KeyCache"
  final val ValuesCacheKey = "ValuesCache"
  final val LookupCacheKey = "LookupCache"
  final val MaxTimestampSeenKey = "MaxTimestampSeen"
  final val GcTimerKey = "GcTimer"
}

class LookupCacheDoFn[K, V, Lookup](timeToLive: Duration)(
    implicit keyCoder: Coder[K],
    leftCoder: Coder[V],
    rightCoder: Coder[(Instant, Lookup)]
) extends LookupCacheDoFnType[K, V, Lookup]
    with LazyLogging {

  require(timeToLive.isLongerThan(Duration.ZERO))

  import LookupCacheDoFn._

  // noinspection ScalaUnusedSymbol
  @StateId(KeyCacheKey) private val keyCacheSpec = StateSpecs.value[K](CoderMaterializer.beamWithDefault(Coder[K]))

  // noinspection ScalaUnusedSymbol
  @StateId(ValuesCacheKey) private val valuesCacheSpec = StateSpecs.bag[V](CoderMaterializer.beamWithDefault(Coder[V]))

  // noinspection ScalaUnusedSymbol
  @StateId(LookupCacheKey) private val lookupCacheSpec =
    StateSpecs.value[(Instant, Lookup)](CoderMaterializer.beamWithDefault(Coder[(Instant, Lookup)]))

  // noinspection ScalaUnusedSymbol
  @StateId(MaxTimestampSeenKey) private val maxTimestampSeenSpec =
    StateSpecs.combining(CoderMaterializer.beamWithDefault(Coder[Holder[Instant]]), new MaxInstantFn())

  // noinspection ScalaUnusedSymbol
  @TimerId(GcTimerKey) private val gcTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME)

  @ProcessElement
  def processElement(
      @Timestamp timestamp: Instant,
      @Element element: InputType[K, V, Lookup],
      @AlwaysFetched @StateId(KeyCacheKey) keyCacheState: ValueState[K],
      @AlwaysFetched @StateId(ValuesCacheKey) valuesCacheState: BagState[V],
      @AlwaysFetched @StateId(LookupCacheKey) lookupCacheState: LookupStateType[Lookup],
      @AlwaysFetched @StateId(MaxTimestampSeenKey) maxTimestampSeenState: MaxTimestampStateType,
      @TimerId(GcTimerKey) gcTimer: Timer,
      receiver: OutputReceiver[OutputType[K, V, Lookup]]
  ): Unit = {
    val (values, lookups) = element.getValue

    // process non-empty element only, empty element is emitted when watermark is advanced to infinity (e.g in tests)
    if (!values.isEmpty || !lookups.isEmpty) {
      logger.debug("Process: {}, {}", timestamp, element)

      val key = element.getKey

      val lookup = cacheAndGetLookup(timestamp, lookups, lookupCacheState)

      if (lookup.isEmpty) {
        // put key and current values to the cache if lookup element has not been seen yet
        keyCacheState.write(key)
        values.foreach { value => valuesCacheState.add(value) }
      } else {
        // output cached element values with lookup element
        outputCachedValues(timestamp, key, valuesCacheState, lookup, receiver)

        // output current element values with lookup element
        outputValues(timestamp, key, values, lookup, receiver)
      }

      // always update gc timer based on maximum timestamp seen
      // this will keep overwriting the timer as long as there is an activity on the key
      // once the key goes inactive then gc timer will fire
      updateGcTimer(timestamp, maxTimestampSeenState, gcTimer)
    }
  }

  @OnTimer(GcTimerKey)
  def onGcTimer(
      @Timestamp timestamp: Instant,
      @AlwaysFetched @StateId(KeyCacheKey) keyCache: ValueState[K],
      @AlwaysFetched @StateId(ValuesCacheKey) valuesCache: BagState[V],
      @AlwaysFetched @StateId(LookupCacheKey) lookupCache: LookupStateType[Lookup],
      @AlwaysFetched @StateId(MaxTimestampSeenKey) maxTimestampSeen: MaxTimestampStateType,
      receiver: OutputReceiver[OutputType[K, V, Lookup]]
  ): Unit = {
    logger.debug("GcTimer: {}", timestamp)
    // emit values without lookup
    val key = keyCache.read()
    outputCachedValues(timestamp, key, valuesCache, None, receiver)

    // free stateful DoFn resources
    valuesCache.clear()
    lookupCache.clear()
    maxTimestampSeen.clear()
  }

  private def cacheAndGetLookup(
      currentTimestamp: Instant,
      currentLookups: Iterable[Lookup],
      lookupCacheState: LookupStateType[Lookup]
  ): Option[Lookup] = {
    val currentLookup = currentLookups.lastOption.map { lookup => (currentTimestamp, lookup) }
    val cachedLookup = Option(lookupCacheState.read())

    (currentLookup, cachedLookup) match {
      case (Some((currentTs, current)), Some((cachedTs, cached))) =>
        if (currentTs.isEqual(cachedTs) || currentTs.isAfter(cachedTs)) {
          // current lookup is the latest, cache and return current
          lookupCacheState.write((currentTs, current))
          Some(current)
        } else {
          // cached lookup is the latest, return cached
          Some(cached)
        }
      case (Some((currentTs, current)), None) =>
        // current lookup is the first one, cache and return current
        lookupCacheState.write((currentTs, current))
        Some(current)
      case (None, Some((_, cached))) =>
        // cached lookup only, return cached
        Some(cached)
      case (None, None) =>
        // no current and cached lookup
        None
    }
  }

  private def outputValues(
      timestamp: Instant,
      key: K,
      values: Iterable[V],
      lookup: Option[Lookup],
      receiver: OutputReceiver[OutputType[K, V, Lookup]]
  ): Unit = {
    values.foreach { value =>
      logger.debug("Output: {}, {}, {}", timestamp, value, lookup)
      receiver.outputWithTimestamp(KV.of(key, (value, lookup)), timestamp)
    }
  }

  private def outputCachedValues(
      timestamp: Instant,
      key: K,
      valuesCache: BagState[V],
      lookup: Option[Lookup],
      receiver: OutputReceiver[OutputType[K, V, Lookup]]
  ): Unit = {
    val cachedValues = valuesCache.read().asScala
    outputValues(timestamp, key, cachedValues, lookup, receiver)

    // clear the cache to free stateful DoFn resources, cached values have been already emitted
    valuesCache.clear()
  }

  private def updateGcTimer(
      currentTimestamp: Instant,
      maxTimestampSeenState: MaxTimestampStateType,
      gcTimer: Timer
  ): Unit = {
    maxTimestampSeenState.add(currentTimestamp)

    val maxTimestampSeen = maxTimestampSeenState.read()
    val expirationTimestamp = maxTimestampSeen.plus(timeToLive)

    // timer can't be set to later timestamp than max Beam timestamp
    val gcTimestamp = if (expirationTimestamp.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
      expirationTimestamp
    } else {
      logger.warn("Could not set GC Timer to {}, fallback to Beam TIMESTAMP_MAX_VALUE", expirationTimestamp)
      BoundedWindow.TIMESTAMP_MAX_VALUE
    }

    // schedule gc timer on gc timestamp but watermark will be held at max timestamp seen until the timer fires
    gcTimer.withOutputTimestamp(maxTimestampSeen).set(gcTimestamp)
  }
}
