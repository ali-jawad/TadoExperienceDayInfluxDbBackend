package com.tado.timeseries

import com.google.common.base.Function
import groovy.transform.CompileStatic
import org.joda.time.Duration
import org.joda.time.Instant
import org.joda.time.Interval

@CompileStatic
final class DataInterval<T> {

   final Interval interval
   final T value

   private DataInterval(Interval interval, T value) {
      this.interval = interval
      this.value = value
   }

   /**
    * @param value might be null to indicte no value for this interval
    */
   static <T> DataInterval<T> di(Interval interval, T value) {
      return new DataInterval<T>(interval, value)
   }

   /**
    * @param value might be null to indicte no value for this interval
    */
   static <T> DataInterval<T> di(Instant start, Instant end, T value) {
      return di(new Interval(start, end), value)
   }

   public <NewType> DataInterval<NewType> transform(Closure<NewType> valueTransformer) {
      return di(interval, valueTransformer(value))
   }

   public <NewType> DataInterval<NewType> transform(Function<T, NewType> valueTransformer) {
      return di(interval, valueTransformer.apply(value))
   }

   Instant getStart() {
      return interval.start.toInstant()
   }

   Instant getEnd() {
      return interval.end.toInstant()
   }

   Duration getDuration() {
      return interval.toDuration()
   }

   boolean equals(o) {
      if (this.is(o)) return true
      if (getClass() != o.class) return false

       DataInterval that = (DataInterval) o

      if (interval != that.interval) return false
      if (value != that.value) return false

      return true
   }

   int hashCode() {
      int result
      result = interval.hashCode()
      result = 31 * result + (value != null ? value.hashCode() : 0)
      return result
   }

   @Override
   String toString() {
      return "($interval: $value)"
   }

}
