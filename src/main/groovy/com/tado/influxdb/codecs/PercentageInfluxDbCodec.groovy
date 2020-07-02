package com.tado.influxdb.codecs

import com.tado.hvaccontrol.Percentage

class PercentageInfluxDbCodec implements InfluxDbCodec<Percentage, Double> {

   @Override
   Double encode(Percentage percentage) {
      return percentage.toDoubleFromZeroToOneHundred()
   }

   @Override
   Percentage decode(Double value) {
      return Percentage.fromZeroToOneHundred(value)
   }

   @Override
   String toString() {
      "percentage"
   }
}
