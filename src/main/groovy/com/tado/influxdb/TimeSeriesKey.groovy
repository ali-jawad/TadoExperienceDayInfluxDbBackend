package com.tado.influxdb

import com.google.common.base.Optional
import com.google.common.collect.ImmutableMap
import groovy.transform.EqualsAndHashCode

@EqualsAndHashCode
final class TimeSeriesKey {
   final String measurement
   final Optional<String> retentionPolicy
   final Map<String, String> tags

   TimeSeriesKey(String measurement, Map<String, ?> tags, String retentionPolicy = null) {
      this.measurement = measurement
      this.retentionPolicy = Optional.fromNullable(retentionPolicy)
      this.tags = ImmutableMap.copyOf(tags.collectEntries {
         return [it.key, it.value.toString()]
      } as Map<String, String>)
   }

   @Override
   String toString() {
      return "${measurement}${tags}"
   }
}
