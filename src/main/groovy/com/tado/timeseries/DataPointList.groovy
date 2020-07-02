package com.tado.timeseries

import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.ImmutableList
import com.google.common.collect.Ordering
import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.FromString
import org.joda.time.Duration
import org.joda.time.Instant
import org.joda.time.Interval

import javax.annotation.Nullable

/**
 * A list of DataPoints. This class guarantees that the data points are always sorted by timestamp.
 */
@CompileStatic
@EqualsAndHashCode(includes = ['points'])
final class DataPointList<ValueType> {

   private final List<DataPoint<ValueType>> points

   private DataPointList(List<DataPoint<ValueType>> points) {
      this.points = ImmutableList.copyOf(points)
   }

   static <T> DataPointList<T> fromPoints(DataPoint<T>... points) {
      return fromList(points.toList())
   }

   static <T> DataPointList<T> fromList(List<DataPoint<T>> points) {
      return from(points.sort(false, { DataPoint<T> dp -> dp.timestamp }))
   }

   static <T> DataPointList<T> fromSortedList(List<DataPoint<T>> points) {
      assert Ordering.natural().isOrdered(points*.timestamp): "list of points is not sorted by timestamp"

      return from(points)
   }

   static <T> DataPointList<T> empty() {
      return from([])
   }

   static <T> DataPointList<T> grid(Instant start, Instant end, Duration bucketSize, T value) {
      def emptyBuckets = [] as List<DataPoint<T>>;

      long bucketMillis = bucketSize.millis
      long startBucket = Math.floor(start.millis / bucketMillis as double) * bucketMillis as long
      long lastBucket = Math.ceil(end.millis / bucketMillis as double) * bucketMillis as long
      long bucket = startBucket

      while (bucket <= lastBucket) {
         emptyBuckets << DataPoint.dp(new Instant(bucket), value)
         bucket += bucketMillis
      }

      return from(emptyBuckets)
   }

   private static <T> DataPointList<T> from(List<DataPoint<T>> points) {
      return new DataPointList<T>(points)
   }

   boolean isEmpty() {
      return points.empty
   }

   int size() {
      return points.size()
   }

    DataPointList<ValueType> startWith(Instant timestamp, ValueType value) {
      def firstPoint = DataPoint.dp(timestamp, value)
      if (points.empty) {
         return from([firstPoint])
      } else if (points.first().timestamp == timestamp) {
         // we already have a point at the start => do not change this DPL
         return this
      } else {
         assert timestamp < points.first().timestamp
         return from([firstPoint] + points)
      }
   }

   DataIntervalPartition<ValueType> toDataIntervalPartition(Instant end) {
      assert !points.empty: "no points"
      assert !(points.size() == 1 && points.last().timestamp == end): "there only is one point right at $end"
      assert last().timestamp <= end: "end ($end) is before last point (${last().timestamp})"

      def pointsToInclude = points.last().timestamp == end ? points.take(points.size() - 1) : points

      // the value for the last DP in the second list does not matter, we are just interested in its timestamp.
      def pointPairs = [pointsToInclude, pointsToInclude.tail() + [DataPoint.dp(end, null)]].transpose() as List<List<DataPoint>>;
      return DataIntervalPartition.createPartition(pointPairs.collect { List<DataPoint> pairs ->
         return DataInterval.di(pairs[0].timestamp, pairs[1].timestamp, pairs[0].value)
      } as List<DataInterval<ValueType>>)
   }

   List<DataPoint<ValueType>> toList() {
      return points
   }

   public <NewValueType> DataPointList<NewValueType> transformValues(@ClosureParams(value = FromString, options = ["ValueType"]) Closure<NewValueType> valueTransformer) {
      return from(points.collect { it.transform(valueTransformer) })
   }

    DataPointList<ValueType> findAll(@ClosureParams(value = FromString, options = ["ValueType"]) Closure<Boolean> valuePredicate) {
      return from(points.findAll { valuePredicate(it.value) })
   }

    DataPointList<ValueType> or(DataPointList<ValueType> alternative) {
      if (points.isEmpty()) {
         return alternative
      } else {
         return this
      }
   }

    DataPointList<ValueType> shift(Duration duration) {
      return from(points.collect { DataPoint.dp(it.timestamp + duration, it.value) })
   }

   // convenience method for testing
   @VisibleForTesting
   Map<Long, ValueType> toUnits(Duration baseLength, Instant timeZero) {
      return points.collectEntries { DataPoint<ValueType> dp ->
         int inUnit = ((dp.timestamp.millis - timeZero.millis) / baseLength.millis) as int
         return [(inUnit): dp.value]
      }
   }

   DataPoint<ValueType> min() {
      return points.min { it.value }
   }

   DataPoint<ValueType> max() {
      return points.max { it.value }
   }

   @Nullable
   ValueType getAt(Instant timestamp) {
      points.find { it.timestamp == timestamp }?.value
   }

   DataPoint<ValueType> first() {
      return points.first()
   }

   DataPoint<ValueType> last() {
      return points.last()
   }

    DataPointList<ValueType> merge(List<DataPoint<ValueType>> additionalPoints) {
      assert additionalPoints.every { this[it.timestamp] == null }

      // this method does implicit sorting by timestamp
      return fromList(points + additionalPoints)
   }

    DataPointList<ValueType> cut(Interval interval) {
      from(points.findAll { interval.contains(it.timestamp) })
   }

    DataPointList<ValueType> withoutLast() {
      assert !points.empty
      from(points.take(points.size() - 1))
   }

    DataPointList<ValueType> removeAll(Collection<Interval> intervalsToRemove) {
      from(points.findAll {
         def dp -> intervalsToRemove.every { !it.contains(dp.timestamp) }
      })
   }

}
