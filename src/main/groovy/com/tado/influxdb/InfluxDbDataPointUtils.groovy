package com.tado.influxdb

import com.tado.timeseries.DataPoint
import com.tado.timeseries.DataPointList
import org.influxdb.dto.QueryResult
import org.joda.time.Instant

import javax.annotation.Nullable

class InfluxDbDataPointUtils {

   private final static String INFLUXDB_TIME_FIELD_NAME = 'time'
   private final static Closure<Instant> DEFAULT_TIME_DECODER = { new Instant(it as long) }

   private InfluxDbDataPointUtils() {
   }

   static InfluxDbFieldMap parseLastValue(@Nullable QueryResult.Series influxDbLastValueSeries,
                                          Collection<String> fieldNames,
                                          String lastFieldPrefix = InfluxDbRequest.LAST_FIELD_NAME_PREFIX) {
      if (influxDbLastValueSeries == null) {
         return InfluxDbFieldMap.createWithAllNullValues(fieldNames)
      } else {
         assert influxDbLastValueSeries.values.size() == 1
         assert influxDbLastValueSeries.columns.every {
            it.startsWith(lastFieldPrefix) || it == INFLUXDB_TIME_FIELD_NAME
         }

         def renamedFields = influxDbLastValueSeries.columns.collect { it.replaceFirst(lastFieldPrefix, '') }
         return InfluxDbFieldMap.createFrom(renamedFields, influxDbLastValueSeries.values.first()).project(fieldNames)
      }
   }

   static DataPointList<InfluxDbFieldMap> createListFromSeries(@Nullable QueryResult.Series influxDbSeries,
                                                               Closure<Instant> timeDecoder = DEFAULT_TIME_DECODER) {
      if (influxDbSeries == null) {
         return DataPointList.empty()
      }

      def allFieldNames = influxDbSeries.columns
      def timeIndex = allFieldNames.indexOf(INFLUXDB_TIME_FIELD_NAME)

      def points = influxDbSeries.values.collect { allValues ->
         def time = timeDecoder(allValues[timeIndex])

         return DataPoint.dp(time, InfluxDbFieldMap.createFrom(allFieldNames, allValues))
      }

      return DataPointList.fromSortedList(points)
   }

}
