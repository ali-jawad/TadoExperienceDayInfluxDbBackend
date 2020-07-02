package com.tado.timeseries

import com.google.common.base.Function
import groovy.transform.CompileStatic
import org.joda.time.DateTime
import org.joda.time.Instant
import org.joda.time.Minutes

/**
 * Consists of a timestamp and a value.
 *
 * Comparison is done based on the timestamp, not the value!
 */
@CompileStatic
class DataPoint<T> implements Comparable {

   final Instant timestamp
   final T value

   private DataPoint(Instant timestamp, T value) {
      this.timestamp = timestamp
      this.value = value
   }

   static <T> DataPoint<T> dp(DateTime timestamp, T value) {
      return new DataPoint<T>(timestamp.toInstant(), value)
   }

   static <T> DataPoint<T> dp(Instant timestamp, T value) {
      return new DataPoint<T>(timestamp, value)
   }

   static <T> DataPoint<T> dp(long millis, T value) {
      return new DataPoint<T>(new Instant(millis), value)
   }

   boolean isDouble() {
      return value instanceof Double || value instanceof BigDecimal
   }

   public <NewType> DataPoint<NewType> transform(Closure<NewType> valueTransformer) {
      return dp(timestamp, valueTransformer(value))
   }

   public <NewType> DataPoint<NewType> transform(Function<T, NewType> valueTransformer) {
      return dp(timestamp, valueTransformer.apply(value))
   }

   public Minutes ageInMinutes() {
      return Minutes.minutesBetween(this.timestamp, Instant.now())
   }

   boolean equals(o) {
      if (this.is(o)) return true
      if (getClass() != o.class) return false

       DataPoint that = (DataPoint) o

      if (timestamp != that.timestamp) return false
      if (value != that.value) return false

      return true
   }

   int hashCode() {
      int result
      result = timestamp.hashCode()
      result = 31 * result + (value != null ? value.hashCode() : 0)
      return result
   }

   @Override
   String toString() {
      return "($timestamp: $value)"
   }

   @Override
   int compareTo(Object o) {
      assert o instanceof DataPoint
      def oInstant = o as DataPoint
      return this.timestamp <=> oInstant.timestamp
   }
}
