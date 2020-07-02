package com.tado.influxdb.codecs

class IdentityInfluxDbCodec<InfluxDbType> implements InfluxDbCodec<InfluxDbType, InfluxDbType> {

   @Override
   InfluxDbType encode(InfluxDbType domainObject) {
      return domainObject
   }

   @Override
   InfluxDbType decode(InfluxDbType value) {
      return value
   }

   @Override
   String toString() {
      "identity"
   }
}
