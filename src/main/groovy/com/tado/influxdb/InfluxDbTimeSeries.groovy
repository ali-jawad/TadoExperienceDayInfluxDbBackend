package com.tado.influxdb

import com.tado.influxdb.codecs.InfluxDbCodec
import com.tado.influxdb.codecs.IntegerInfluxDbCodec
import com.tado.influxdb.codecs.LongInfluxDbCodec
import com.tado.influxdb.codecs.MapInfluxDbCodec
import groovy.transform.EqualsAndHashCode
import groovy.util.logging.Slf4j

@Slf4j
@EqualsAndHashCode(includeFields = true, includes = ['name'])
final class InfluxDbTimeSeries<DomainType, InfluxDbType> {

   final static String INFLUXDB_HIERARCHY_SEPARATOR = '.'

   private final String name
   private final InfluxDbCodec<DomainType, InfluxDbType> codec

   static <DomainType, InfluxDbType> InfluxDbTimeSeries<DomainType, InfluxDbType> timeSeries(String name, InfluxDbCodec<DomainType, InfluxDbType> codec) {
      return new InfluxDbTimeSeries<DomainType, InfluxDbType>(name, codec)
   }

   private InfluxDbTimeSeries(String name, InfluxDbCodec<DomainType, InfluxDbType> codec) {
      this.name = name
      this.codec = codec
   }

   String getName() {
      return name
   }

   List<String> getFieldNames() {
      return codec instanceof MapInfluxDbCodec ?
         codec.fieldNames.collect { name + INFLUXDB_HIERARCHY_SEPARATOR + it } :
         [name]
   }

   DomainType decode(InfluxDbType value) {
      if (value == null) {
         log.warn("trying to decode single valued time series: $name with codec: $codec passing null valued field")
         return null
      }

      if (codec instanceof IntegerInfluxDbCodec) {
         // InfluxDB Java client returns doubles also for int fields (since JSON does not now 'int', just 'number')
         return codec.decode(value as Integer)
      } else if (codec instanceof LongInfluxDbCodec) {
         // InfluxDB Java client returns doubles also for long (int 64) fields (since JSON does not now 'long', just 'number')
         return codec.decode(value as Long)
      } else {
         return codec.decode(value as InfluxDbType)
      }
   }

   InfluxDbType encode(DomainType domainObject) {
      codec.encode(domainObject)
   }

   @Override
   String toString() {
      "time series '$name' (codec: $codec)"
   }
}
