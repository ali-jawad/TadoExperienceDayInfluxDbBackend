package com.tado.timeseries

import com.google.common.annotations.VisibleForTesting
import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.FromString
import org.joda.time.Duration
import org.joda.time.Instant
import org.joda.time.Interval

/**
 * A list of DataIntervals which partition a time interval. This class guarantees:
 * - no overlapping data intervals
 * - no gaps between data intervals
 * - no consecutive data intervals with the same values. Consecutive intervals with the same value will be merged into one.
 */
@CompileStatic
@EqualsAndHashCode(includes = ['partitions'])
final class DataIntervalPartition<T> {

   private final List<DataInterval<T>> partitions

   static <T> DataIntervalPartition<T> createPartition(DataInterval<T>... partitions) {
      return createPartition(partitions.toList())
   }

   static <T> DataIntervalPartition<T> createPartition(List<DataInterval<T>> partitions) {
      return new DataIntervalPartition<T>(partitions)
   }

   private DataIntervalPartition(List<DataInterval<T>> partitions) {
      Preconditions.checkArgument(!partitions.empty, "DIP needs to include at least one interval")

      Preconditions.checkArgument(partitions.every {
         it.start != it.end
      }, "there are zero-length intervals")

      def combinedIntervals = [
         partitions.subList(0, partitions.size() - 1),
         partitions.subList(1, partitions.size())
      ].transpose() as List<List<DataInterval<?>>>;
      Preconditions.checkArgument(combinedIntervals.every { List<DataInterval<?>> dis -> dis[0].end == dis[1].start },
         "intervals have gaps or overlap")

      def newIntervals = [partitions.first()]
      for (int i = 1; i < partitions.size(); i++) {
         if (newIntervals.last().value == partitions[i].value) {
            newIntervals[newIntervals.size() - 1] = com.tado.timeseries.DataInterval.di(
               newIntervals[newIntervals.size() - 1].start,
               partitions[i].end,
               partitions[i].value)
         } else {
            newIntervals << partitions[i]
         }
      }

      this.partitions = ImmutableList.copyOf(newIntervals)
   }

   Instant getStart() {
      return partitions.first().start
   }

   Instant getEnd() {
      return partitions.last().end
   }

   DataInterval<T> getSingleInterval() {
      assert size() == 1
      return partitions.first()
   }

   int size() {
      return partitions.size()
   }

   List<DataInterval<T>> getPartitions() {
      return partitions
   }

   List<Interval> findPartitions(T value) {
      return partitions.findAll { it.value == value }*.interval
   }

   List<DataInterval<T>> findPartitions(@ClosureParams(value = FromString, options = ["T"]) Closure<Boolean> valuePredicate) {
      return partitions.findAll { valuePredicate(it.value) }
   }

   DataInterval<T> getIntervalAt(Instant instant) {
      assert start <= instant && instant < end

      partitions.find { it.start <= instant && instant < it.end }

   }

   Duration calculateDuration(@ClosureParams(value = FromString, options = ["T"]) Closure<Boolean> valuePredicate = { true }) {
      // @formatter:off
      return findPartitions(valuePredicate)
         .inject(Duration.ZERO) { Duration total, DataInterval interval ->
            return total + interval.duration
         } as Duration
      // @formatter:on
   }

   T getValueAt(Instant instant) {
      return getIntervalAt(instant).value
   }

   public <V> DataIntervalPartition<V> transformValues(@ClosureParams(value = FromString, options = ["T"]) Closure<V> valueTransformer) {
      return createPartition(partitions.collect { it.transform(valueTransformer) })
   }

    DataIntervalPartition<T> replaceNullValues(T replacementValue) {
      // using `replacementValue` instead of `null` might have created consecutive intervals with same value
      return transformValues { it == null ? replacementValue : it }
   }

    DataIntervalPartition<T> cut(Interval cutInterval) {
      assert !partitions.empty
      Preconditions.checkArgument(cutInterval.start >= start, "cut interval starts before data interval partition does")
      Preconditions.checkArgument(cutInterval.end <= end, "cut interval ends after data interval partition does")

      List<DataInterval<T>> newIntervals = []
      partitions.each {
         if (it.end <= cutInterval.start || it.start >= cutInterval.end) {
            // ignore these
         } else if (cutInterval.contains(it.interval)) {
            newIntervals << it
         } else {
            newIntervals << com.tado.timeseries.DataInterval.di(
               [cutInterval.start.toInstant(), it.start].max(),
               [cutInterval.end.toInstant(), it.end].min(),
               it.value
            )
         }
      }
      return createPartition(newIntervals)
   }

    DataIntervalPartition<T> cutStart(Instant newStart) {
      def start = newStart < start ? start : newStart
      return cut(new Interval(start, end))
   }

   /**
    * @return a DIP, with all the values of this DIP, except where punchRanges is true. For these ranges, the returned
    * DIP contains the replacementValue.
    */
   DataIntervalPartition<T> punch(DataIntervalPartition<Boolean> punchRanges, T replacementValue) {
      return crossProduct([orig: this, punch: punchRanges]).transformValues {
         return it['punch'] == true ? replacementValue : it['orig']
      } as DataIntervalPartition<T>
   }

   /**
    * Integrates the transformed partition values. Time unit for the integration is milliseconds.
    *
    * @param toDoubleValueTransformer
    * @return integrated transformed values
    */
   double integrate(@ClosureParams(value = FromString, options = ["T"]) Closure<Double> toDoubleValueTransformer) {
      return partitions.inject(0d, { acc, di ->
         return acc + toDoubleValueTransformer((di.value)) * di.duration.millis
      }) as double
   }

   // convenience method for testing
   @VisibleForTesting
   Map<Long, T> toUnitsAtStart(Duration baseLength, Instant timeZero) {
      return toPointsAtStart().toUnits(baseLength, timeZero)
   }

    DataPointList<T> toPointsAtStart() {
      return DataPointList.fromSortedList(partitions.collect {
         DataPoint.dp(it.start, it.value)
      })
   }

   boolean equals(o) {
      if (this.is(o)) return true
      if (!(o instanceof DataIntervalPartition)) return false

       DataIntervalPartition that = (DataIntervalPartition) o

      if (partitions != that.partitions) return false

      return true
   }

   int hashCode() {
      return (partitions != null ? partitions.hashCode() : 0)
   }

   static DataIntervalPartition<Map<String, ?>> crossProduct(Map<String, DataIntervalPartition<?>> series) {
      assert !series.isEmpty(): "there must be at least one data interval partition for a cross product"

      def dips = series.values()
      def startTimes = dips.collect { it.start } as Set<Instant>
      def endTimes = dips.collect { it.end } as Set<Instant>
      Preconditions.checkArgument(startTimes.size() == 1, "intervals do not start at the same time. Starts: $startTimes")
      Preconditions.checkArgument(endTimes.size() == 1, "intervals do not end at the same time. Ends: $endTimes")

      Instant startTime = startTimes.first().toInstant()
      Instant endTime = endTimes.first().toInstant()

      // in case of empty interval
      if (startTime == endTime) {
         assert dips*.size().every { it == 1 }
         def values = series.collectEntries { String key, DataIntervalPartition<?> dip ->
            [(key): dip.getValueAt(dip.start)]
         } as Map<String, ?>
         return createPartition(com.tado.timeseries.DataInterval.di(startTime, endTime, values))
      }

      def currentTime = startTime
      def seriesHeads = series.collectEntries { key, _ -> [(key): 0] } as Map<String, Integer>;
      def crossed = []
      while (currentTime != endTime) {
         seriesHeads = seriesHeads.collectEntries { key, head ->
            [(key): moveHeadToCurrentInterval(series[key].partitions, head, currentTime)]
         } as Map<String, Integer>;
         def nextTime = seriesHeads.collect { key, head ->
            series[key].partitions[head].end
         }.min() as Instant
         def values = seriesHeads.collectEntries { key, head ->
            [(key): series[key].partitions[head].value]
         } as Map<String, ?>
         crossed << com.tado.timeseries.DataInterval.di(currentTime, nextTime, values)

         currentTime = nextTime
      }

      return createPartition(crossed)
   }

   /**
    * See the test cases as documentation for how this operation does.
    *
    * @param mapValuedDip  A DIP with each interval being a map having the same keys
    * @throws IllegalArgumentException if not all intervals in the DIP have the same map keys
    */
   static <KeyType, ValueType> Map<KeyType, DataIntervalPartition<ValueType>> spread(DataIntervalPartition<Map<KeyType, ValueType>> mapValuedDip) {
      def keys = mapValuedDip.partitions.first().value.keySet()
      if (!mapValuedDip.partitions.every { it.value.keySet() == keys }) {
         throw new IllegalArgumentException("not every interval in $mapValuedDip has the keys ${mapValuedDip.partitions.first().value.keySet()}")
      }

      def dips = keys.collectEntries { return [it, []] } as Map<KeyType, List<DataInterval<ValueType>>>;

      mapValuedDip.partitions.each { def partition ->
         keys.each { KeyType key ->
            dips[key] << com.tado.timeseries.DataInterval.di(partition.interval, partition.value[key])
         }
      }

      return dips.collectEntries {
         [(it.key): createPartition(it.value)]
      } as Map<KeyType, DataIntervalPartition<ValueType>>
   }

   private static Integer moveHeadToCurrentInterval(List<DataInterval<?>> partitions, Integer head, Instant instant) {
      def partition = partitions[head]
      if (partition.interval.contains(instant)) {
         return head
      } else {
         return head + 1
      }
   }

   @Override
   String toString() {
      return partitions.toString()
   }
}
