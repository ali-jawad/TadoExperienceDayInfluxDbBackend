package com.tado.influxdb.codecs

import com.tado.hvaccontrol.Voltage
import groovy.transform.CompileStatic

@CompileStatic
final class VoltageInfluxDbCodec implements InfluxDbCodec<Voltage, Double> {

   @Override
   Double encode(Voltage voltage) {
      return voltage.volt as double
   }

   @Override
   Voltage decode(Double value) {
      return Voltage.fromVolt(value)
   }

}
