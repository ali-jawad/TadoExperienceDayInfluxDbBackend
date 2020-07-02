package com.tado.influxdb

import com.netflix.hystrix.HystrixCollapser
import com.netflix.hystrix.HystrixCollapserKey
import com.netflix.hystrix.HystrixCollapserProperties
import com.netflix.hystrix.HystrixCommand
import org.influxdb.InfluxDB
import org.influxdb.dto.Point

import javax.annotation.Nullable

class WriteInfluxDbCollapser extends HystrixCollapser<Void, Void, Point> {

   private final InfluxDB influxDb
   private final String database
   private final Point point
   private final String retentionPolicy

   WriteInfluxDbCollapser(InfluxDB influxDb, String database, Point point, int maxRequestsInBatch, @Nullable String retentionPolicy) {
      super(
         HystrixCollapser.Setter.withCollapserKey(HystrixCollapserKey.Factory.asKey("WriteInfluxDbCollapser${retentionPolicy ?: ''}"))
            .andScope(HystrixCollapser.Scope.GLOBAL)
            .andCollapserPropertiesDefaults(
            // http://neidetcher.com/programming/2013/10/17/getting-around-groovy-linkage-error.html
            (HystrixCollapserProperties.invokeMethod("Setter", null) as HystrixCollapserProperties.Setter)
            // TODO: make this configurable through Config.groovy?
               .withMaxRequestsInBatch(maxRequestsInBatch)
         )
      )

      this.influxDb = influxDb
      this.database = database
      this.point = point
      this.retentionPolicy = retentionPolicy
   }

   @Override
   Point getRequestArgument() {
      return point
   }

   @Override
   protected HystrixCommand<Void> createCommand(Collection<HystrixCollapser.CollapsedRequest<Void, Point>> requests) {
      return new WriteInfluxDbPointsCommand(influxDb, database, requests*.argument, retentionPolicy)
   }

   @Override
   protected void mapResponseToRequests(Void response, Collection<HystrixCollapser.CollapsedRequest<Void, Point>> requests) {
      requests.each { it.setResponse(response) }
   }

   @Override
   String toString() {
      return "collapse writing of point $point to database '$database' of InfluxDB"
   }

}
