package com.tado.hvaccontrol

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode

@EqualsAndHashCode
@CompileStatic
final class Voltage implements Comparable<Voltage> {

   final int milliVolt

   private Voltage(int milliVolt) {
      this.milliVolt = milliVolt
   }

   static Voltage fromVolt(double volt) {
      return new Voltage((volt * 1000) as int)
   }

   static Voltage fromVolt(BigDecimal volt) {
      return new Voltage((volt * 1000) as int)
   }

   static Voltage fromMilliVolt(int milliVolt) {
      return new Voltage(milliVolt)
   }

   int getMilliVolt() {
      return milliVolt
   }

   BigDecimal getVolt() {
      return milliVolt / 1000
   }

   @Override
   String toString() {
      return "$milliVolt mV"
   }

   @Override
   int compareTo(Voltage o) {
      return ((Integer) this.milliVolt).compareTo(o.milliVolt)
   }
}
