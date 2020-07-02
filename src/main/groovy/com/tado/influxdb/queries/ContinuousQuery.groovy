package com.tado.influxdb.queries

import org.joda.time.Period

class ContinuousQuery<DomainType> extends Query<DomainType> {
   enum Aggregation {
      MEAN
   }

   Period bucketSize
   Aggregation aggregation

   @Override
   public String toString() {
      return "${super.toString()}, type: continous, bucketSize: $bucketSize, aggregation: $aggregation"
   }
}
