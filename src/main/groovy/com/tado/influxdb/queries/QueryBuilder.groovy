package com.tado.influxdb.queries

import com.tado.influxdb.InfluxDbTimeSeries
import com.tado.influxdb.TimeSeriesKey
import org.joda.time.Period

/**
 * Usage: forTimeSeries(...)
 *          .forInfluxDbTimeSeries(...)
 *          .discrete() or .continuous(...) or .raw()
 *
 *       And you get a fully built query.
 */
class QueryBuilder<DomainType>  {

   TimeSeriesKey key
   InfluxDbTimeSeries<DomainType, ?> timeSeries

   static QueryBuilder forTimeSeries(String measurement, Map<String, String> tags, String retentionPolicy = null) {
      return new QueryBuilder(key: new TimeSeriesKey(measurement, tags, retentionPolicy))
   }

   static QueryBuilder forTimeSeries(TimeSeriesKey key) {
      return new QueryBuilder(key: key)
   }

   public <DomainType> QueryBuilder<DomainType> forInfluxDbTimeSeries(InfluxDbTimeSeries<DomainType, ?> timeSeries) {
      this.timeSeries = timeSeries
      return this
   }

    DiscreteStateQuery<DomainType> discrete() {
      return new DiscreteStateQuery<DomainType>(key: key, timeSeries: timeSeries)
   }

    ContinuousQuery<DomainType> continuous(Period bucketSize, ContinuousQuery.Aggregation aggregation) {
      assert timeSeries.fieldNames.size() == 1
      return new ContinuousQuery<DomainType>(key: key, timeSeries: timeSeries, bucketSize: bucketSize, aggregation: aggregation)
   }

    RawQuery<DomainType> raw() {
      return new RawQuery<DomainType>(key: key, timeSeries: timeSeries)
   }

}
