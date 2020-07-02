package com.tado.hvaccontrol

import groovy.transform.CompileStatic

import java.math.RoundingMode

@CompileStatic
final class Percentage implements Serializable, Comparable<Percentage> {

   final static Percentage ZERO = fromZeroToOne(0)
   final static Percentage HUNDRED = fromZeroToOne(1)

   private final static int precision = 3

   /**
    * value is between 0.0 and 1.0 (inclusive) with 3 digits after the decimal point.
    */
   private final BigDecimal value

   private Percentage(BigDecimal value) {
      this.value = value
   }

   static Percentage zero() {
      return ZERO
   }

   static Percentage fromZeroToOne(BigDecimal percentage) {
      assert percentage >= 0d || percentage <= 1.0d
      return new Percentage(percentage.setScale(precision, BigDecimal.ROUND_HALF_UP))
   }

   static Percentage fromZeroToOne(double percentage) {
      assert percentage >= 0d || percentage <= 1.0d
      return fromZeroToOne(new BigDecimal(String.valueOf(percentage)))
   }

   static Percentage fromZeroToOneHundred(double percentage) {
      assert percentage >= 0d || percentage <= 100.0d
      return fromZeroToOne(percentage / 100)
   }

   static Percentage fromZeroToOneHundred(int percentage) {
      assert percentage >= 0 || percentage <= 100
      return fromZeroToOne(percentage / 100)
   }

   double toDoubleFromZeroToOne() {
      return value.doubleValue()
   }

   double toDoubleFromZeroToOneHundred() {
      return (value * 100).doubleValue()
   }

   int toIntFromZeroToOneHundred() {
      return (value * 100).setScale(0, RoundingMode.HALF_UP).intValue()
   }

   int toIntFromZeroToOneThousand() {
      return (value * 1000).setScale(0, RoundingMode.HALF_UP).intValue()
   }

   @Override
   boolean equals(o) {
      if (this.is(o)) return true
      if (getClass() != o.class) return false

      Percentage that = (Percentage) o

      if (value != that.value) return false

      return true
   }

   @Override
   int hashCode() {
      return value.hashCode()
   }

   @Override
   String toString() {
      return "${toDoubleFromZeroToOneHundred()} %"
   }

   @Override
   int compareTo(Percentage o) {
      return value <=> o.value
   }
}
