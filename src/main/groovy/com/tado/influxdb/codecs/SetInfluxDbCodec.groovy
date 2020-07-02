package com.tado.influxdb.codecs

import groovy.json.JsonOutput
import groovy.json.JsonSlurper


class SetInfluxDbCodec<DomainType> implements InfluxDbCodec<Set<DomainType>, String> {

   private final InfluxDbCodec<DomainType, ?> valueCodec

   SetInfluxDbCodec(InfluxDbCodec<DomainType, ?> valueCodec) {
      this.valueCodec = valueCodec
   }

   @Override
   String encode(Set<DomainType> set) {
      return JsonOutput.toJson(set.collect { valueCodec.encode(it) })
   }

   @Override
   Set<DomainType> decode(String value) {
      return (new JsonSlurper().parseText(value) as Set).collect { valueCodec.decode(it) }
   }
}
