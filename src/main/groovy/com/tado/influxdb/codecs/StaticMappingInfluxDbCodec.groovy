package com.tado.influxdb.codecs

import com.google.common.collect.BiMap
import com.google.common.collect.HashBiMap

abstract class StaticMappingInfluxDbCodec<DomainType, InfluxDbType> implements InfluxDbCodec<DomainType, InfluxDbType> {

   private final BiMap<DomainType, InfluxDbType> mappings

   StaticMappingInfluxDbCodec(Map<DomainType, InfluxDbType> mappings) {
      this.mappings = HashBiMap.create(mappings)
   }

   @Override
   InfluxDbType encode(DomainType domainObject) {
      return mappings[domainObject]
   }

   @Override
   DomainType decode(InfluxDbType value) {
      return mappings.inverse()[value]
   }

   @Override
   String toString() {
      "static mapping ($mappings)"
   }
}

class StaticMappingIntCodec<DomainType> extends StaticMappingInfluxDbCodec<DomainType, Integer> implements IntegerInfluxDbCodec<DomainType> {
   StaticMappingIntCodec(Map<DomainType, Integer> mappings) {
      super(mappings)
   }
}

class StaticMappingBooleanCodec<DomainType> extends StaticMappingInfluxDbCodec<DomainType, Boolean> {
   StaticMappingBooleanCodec(Map<DomainType, Boolean> mappings) {
      super(mappings)
   }
}
