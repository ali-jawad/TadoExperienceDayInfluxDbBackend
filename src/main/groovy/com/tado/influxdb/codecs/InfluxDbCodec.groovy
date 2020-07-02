package com.tado.influxdb.codecs

interface InfluxDbCodec<DomainType, InfluxDbType> {

   /**
    * @return must not be null!
    */
   InfluxDbType encode(DomainType domainObject)

   /**
    * @param value will never be null!
    */
   DomainType decode(InfluxDbType value)

}
