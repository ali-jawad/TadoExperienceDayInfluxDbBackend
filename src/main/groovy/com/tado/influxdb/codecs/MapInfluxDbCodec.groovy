package com.tado.influxdb.codecs

import com.tado.influxdb.InfluxDbTimeSeries

final class MapInfluxDbCodec<DomainType> implements InfluxDbCodec<DomainType, Map<String, ?>> {

   private final Map<String, InfluxDbTimeSeries> nestedTimeSeries
   private final Closure<DomainType> composer
   private final Closure<Map<String, ?>> splitter

   /**
    * @param nestedTimeSeries The Time Series composing this multi-value timeseries
    * @param splitter Closure that takes an object and turns it into a Map.
    *          The key names of the map should be a 1-1 mapping with the nestedTimeSeries names.
    * @param composer Closure that takes a Map and composes the object back using it.
    *          The key names of the map should be a 1-1 mapping with the nestedTimeSeries names.
    * @return The multi-value timeseries that can encode and decode the object taken by and returned
    *          by the splitter and composer respectively.
    */
   static <DomainType> MapInfluxDbCodec<DomainType> mapCodec(Collection<InfluxDbTimeSeries> nestedTimeSeries, Closure<Map<String, ?>> splitter, Closure<DomainType> composer) {
      return new MapInfluxDbCodec<DomainType>(nestedTimeSeries, splitter, composer)
   }

   private MapInfluxDbCodec(Collection<InfluxDbTimeSeries> nestedTimeSeries, Closure<Map<String, ?>> splitter, Closure<DomainType> composer) {
      this.nestedTimeSeries = nestedTimeSeries
         .groupBy { it.name }
      // @formatter:off
         .collectEntries { String nestedName, List<InfluxDbTimeSeries> timeSeries ->
            assert timeSeries.size() == 1: "more than one time series with name '$nestedName' added to $this"
            [(nestedName): timeSeries.first()]
         } as Map<String, InfluxDbTimeSeries>
      // @formatter:on
      this.splitter = splitter
      this.composer = composer
   }

   Collection<String> getFieldNames() {
      return nestedTimeSeries.keySet()
   }

   @Override
   Map<String, ?> encode(DomainType domainObject) {
      splitter(domainObject).collectEntries { String nestedName, def value ->
         assert nestedTimeSeries[nestedName] != null: "no nested time series $nestedName found for $this"
         def nestedValue
         if (value == null) {
            // nested time series should not expect null values --> do not encode `null`
            nestedValue = null
         } else {
            // This returns a map, which is a valid response for the outer collectEntries closure --> Groovy merges the maps.
            nestedValue = nestedTimeSeries[nestedName].encode(value)
         }
         return [(nestedName): nestedValue]
      } as Map<String, ?>
   }

   @Override
   DomainType decode(Map<String, ?> map) {
      def decodedMap = map.collectEntries { String name, Object value ->
         // nested time series should not expect null values --> do not decode `null`
         [(name): value == null ? null : nestedTimeSeries[name].decode(value)]
      } as Map<String, Object>

      return composer(decodedMap)
   }

   @Override
   String toString() {
      "map codec, with nested time series ${nestedTimeSeries.values()}"
   }
}
