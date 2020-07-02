package com.tado.influxdb

import com.tado.influxdb.queries.ContinuousQuery
import com.tado.influxdb.queries.DiscreteStateQuery
import com.tado.influxdb.queries.GroupedQuery
import com.tado.influxdb.queries.RawQuery
import com.tado.timeseries.DataIntervalPartition
import com.tado.timeseries.DataPointList
import groovy.transform.CompileStatic
import org.joda.time.Instant

@CompileStatic
class InfluxDbResult {
   private Instant end

   private Map<TimeSeriesKeyAndName, DataPointList<InfluxDbFieldMap>> discreteStateResults = [:]
   private Map<TimeSeriesKeyAndName, DataPointList<InfluxDbFieldMap>> rawResults = [:]
   private Map<TimeSeriesKeyAndName, DataPointList<InfluxDbFieldMap>> continuousResults = [:]

   InfluxDbResult(Instant end, Map<TimeSeriesKeyAndName, DataPointList<InfluxDbFieldMap>> discreteStateResults, Map<TimeSeriesKeyAndName, DataPointList<InfluxDbFieldMap>> rawResults, Map<TimeSeriesKeyAndName, DataPointList<InfluxDbFieldMap>> continuousResults) {
      this.end = end
      this.discreteStateResults = discreteStateResults
      this.rawResults = rawResults
      this.continuousResults = continuousResults
   }

   public <T> DataPointList<T> get(RawQuery<T> query) {
      return rawResults[query.keyAndName].transformValues { it.decode(query.timeSeries) }
   }

   public <T> DataPointList<T> get(ContinuousQuery<T> query) {
      return continuousResults[query.keyAndName].transformValues { it.decode(query.timeSeries) }
   }

   public <T> DataIntervalPartition<T> get(DiscreteStateQuery<T> query) {
      return discreteStateResults[query.keyAndName].transformValues { it.decode(query.timeSeries) }
         .toDataIntervalPartition(end)
   }

   public <T> Map<String, DataPointList<T>> get(GroupedQuery<T> query) {
      query.groupByTagValues.collectEntries {
         def keyAndName = query.getSubKeyAndName(it)

         def groupedQuery = query.groupedQuery
         if (groupedQuery instanceof DiscreteStateQuery) {
            def decodedResult = discreteStateResults[keyAndName].transformValues { it.decode(groupedQuery.timeSeries) }
            return [it, decodedResult]
         } else if (groupedQuery instanceof RawQuery) {
            def decodedResult = rawResults[keyAndName].transformValues { it.decode(groupedQuery.timeSeries) }
            return [it, decodedResult]
         } else {
            throw new UnsupportedOperationException("grouping by query type ${groupedQuery.class} not supported")
         }
      }
   }

}
