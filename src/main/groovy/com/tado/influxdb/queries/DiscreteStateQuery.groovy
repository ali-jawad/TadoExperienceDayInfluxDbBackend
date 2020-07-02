package com.tado.influxdb.queries

class DiscreteStateQuery<DomainType> extends Query<DomainType> {
   @Override
   public String toString() {
      return "${super.toString()}, type: discrete"
   }
}
