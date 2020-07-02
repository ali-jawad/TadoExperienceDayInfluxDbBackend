package com.tado.influxdb

import groovy.transform.EqualsAndHashCode

@EqualsAndHashCode
final class TimeSeriesKeyAndName {

   final TimeSeriesKey key
   final String name

   TimeSeriesKeyAndName(TimeSeriesKey key, String name) {
      this.key = key
      this.name = name
   }

    TimeSeriesKeyAndName forSubTag(String tagName, String tagValue) {
      def newKey = new TimeSeriesKey(key.measurement, key.tags + [(tagName): tagValue], key.retentionPolicy.orNull())
      return new TimeSeriesKeyAndName(newKey, name)
   }

   @Override
   String toString() {
      return "$key:$name"
   }
}
