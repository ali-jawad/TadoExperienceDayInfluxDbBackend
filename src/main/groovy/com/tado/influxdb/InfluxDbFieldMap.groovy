package com.tado.influxdb

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode

@CompileStatic
@EqualsAndHashCode(includes = ['fields'])
final class InfluxDbFieldMap {

   private final static String INFLUXDB_TIME_FIELD_NAME = 'time'

   private final Map<String, ?> fields

   private InfluxDbFieldMap(Map<String, ?> fields) {
      this.fields = fields
   }

   static InfluxDbFieldMap createFrom(List<String> columnsIncludingTime, List<?> valuesIncludingTime) {
      def seriesValues = [columnsIncludingTime, valuesIncludingTime].transpose().collectEntries()
      seriesValues.remove(INFLUXDB_TIME_FIELD_NAME)

      return new InfluxDbFieldMap(seriesValues)
   }

   static InfluxDbFieldMap createWithSingleField(String fieldName, def fieldValue) {
      return new InfluxDbFieldMap([(fieldName): fieldValue])
   }

   static InfluxDbFieldMap createWithAllNullValues(Collection<String> fieldNames) {
      return new InfluxDbFieldMap(fieldNames.collectEntries { [it, null] })
   }

   def unwrapSingleValue() {
      assert fields.size() == 1
      fields.values().first()
   }

   boolean allValuesNull() {
      return fields.values().every { it == null }
   }

    InfluxDbFieldMap project(Collection<String> fieldsToRetain) {
      assert fields.keySet().containsAll(fieldsToRetain)
      return new InfluxDbFieldMap(fields.entrySet().findAll { it.key in fieldsToRetain }.collectEntries())
   }

   public <T> T decode(InfluxDbTimeSeries<T, ?> timeSeries) {
      if (allValuesNull()) {
         // Do not pass `null`, or a Map with only `null` values to the time series decoder.
         // This can happen for the first point of discrete state time series, which is retrieved from LAST() queries.
         return null
      }

      if (fields.size() == 1) {
         assert fields.containsKey(timeSeries.name)
         return timeSeries.decode(fields[timeSeries.name])
      } else {
         def result = fields.collectEntries { String key, def value ->
            assert key.startsWith(timeSeries.name + InfluxDbTimeSeries.INFLUXDB_HIERARCHY_SEPARATOR)
            def newKey = key.replaceFirst(timeSeries.name + InfluxDbTimeSeries.INFLUXDB_HIERARCHY_SEPARATOR, '')
            [newKey, value]
         }
         return timeSeries.decode(result)
      }
   }

   @Override
   String toString() {
      return fields.toString()
   }
}
