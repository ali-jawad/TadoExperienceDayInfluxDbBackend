package com.tado.influxdb

import org.influxdb.dto.Point
import org.joda.time.Instant
import org.joda.time.Interval

import java.util.concurrent.TimeUnit

class InfluxDbPointBuilder {

   // see https://docs.influxdata.com/influxdb/v1.2/troubleshooting/errors/#unable-to-parse-time-outside-range
   private final static Interval INFLUX_DB_ALLOWED_RANGE = new Interval(new Instant('1677-09-21T00:12:43.145224194Z'), new Instant('2262-04-11T23:47:16.854775806Z'))

   final Point.Builder builder

   InfluxDbPointBuilder(String measurement, Map<String, String> tags) {
      this.builder = Point
         .measurement(measurement)
         .tag(tags)
   }

    InfluxDbPointBuilder at(Instant time) {
      assert INFLUX_DB_ALLOWED_RANGE.contains(time): "time $time not in allowed InfluxDB range"

      builder.time(time.millis, TimeUnit.MILLISECONDS)
      return this
   }

   private void addFields(String timeSeriesName, def value) {
      if (value == null) {
         // do not write null fields
      } else if (value instanceof Boolean) {
         builder.addField(timeSeriesName, value)
      } else if (value instanceof Long) {
         builder.addField(timeSeriesName, value)
      } else if (value instanceof Double) {
         builder.addField(timeSeriesName, value)
      } else if (value instanceof Number) {
         builder.addField(timeSeriesName, value)
      } else if (value instanceof String) {
         builder.addField(timeSeriesName, value)
      } else if (value instanceof Map<String, ?>) {
         value.each { String subFieldName, def innerValue ->
            addFields(timeSeriesName + InfluxDbTimeSeries.INFLUXDB_HIERARCHY_SEPARATOR + subFieldName, innerValue)
         }
      } else {
         throw new UnsupportedOperationException("cannot add field for value of type ${value.class}")
      }
   }

   public <T, InfluxDbType> InfluxDbPointBuilder add(InfluxDbTimeSeries<T, InfluxDbType> timeSeries, T domainObject) {
      InfluxDbType encodedFields = timeSeries.encode(domainObject)
      addFields(timeSeries.name, encodedFields)
      return this
   }

   boolean hasFields() {
      return builder.hasFields()
   }

   Point build() {
      return builder.build()
   }

}
