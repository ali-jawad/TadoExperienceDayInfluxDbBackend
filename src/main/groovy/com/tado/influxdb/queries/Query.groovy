package com.tado.influxdb.queries

import com.tado.influxdb.InfluxDbTimeSeries
import com.tado.influxdb.TimeSeriesKey
import com.tado.influxdb.TimeSeriesKeyAndName

abstract class Query<DomainType> {
   TimeSeriesKey key

   InfluxDbTimeSeries<DomainType, ?> timeSeries

    TimeSeriesKeyAndName getKeyAndName() {
      return new TimeSeriesKeyAndName(key, timeSeries.name)
   }

   List<String> getFields() {
      return timeSeries.fieldNames
   }

   @Override
   String toString() {
      return "$key (${fields.join(',')})"
   }
}
