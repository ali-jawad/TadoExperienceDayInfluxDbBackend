package com.tado.influxdb.queries

class RawQuery<DomainType> extends Query<DomainType> {
   @Override
   public String toString() {
      return "${super.toString()}, type: raw"
   }
}
