package com.tado.influxdb.codecs

import org.joda.time.Duration

class DurationCodec implements LongInfluxDbCodec<Duration> {

   @Override
   Long encode(Duration duration) {
      return duration.millis
   }

   @Override
   Duration decode(Long millis) {
      return new Duration(millis)
   }

}
