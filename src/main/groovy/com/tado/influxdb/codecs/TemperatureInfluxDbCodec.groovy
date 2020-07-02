package com.tado.influxdb.codecs

import com.tado.hvaccontrol.Temperature

class TemperatureInfluxDbCodec implements IntegerInfluxDbCodec<Temperature> {

   @Override
   Integer encode(Temperature temperature) {
      return temperature.centiCelsius
   }

   @Override
   Temperature decode(Integer centiCelsius) {
      return Temperature.fromCentiCelsius(centiCelsius)
   }

   @Override
   String toString() {
      "temperature"
   }
}
