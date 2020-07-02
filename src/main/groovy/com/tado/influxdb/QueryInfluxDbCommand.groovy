package com.tado.influxdb

import com.netflix.hystrix.HystrixCommand
import com.netflix.hystrix.HystrixCommandGroupKey
import com.netflix.hystrix.HystrixCommandKey
import com.netflix.hystrix.HystrixCommandProperties
import com.netflix.hystrix.HystrixThreadPoolKey
import com.netflix.hystrix.HystrixThreadPoolProperties
import com.netflix.hystrix.exception.HystrixBadRequestException
import org.influxdb.InfluxDB
import org.influxdb.dto.Query
import org.influxdb.dto.QueryResult

import java.util.concurrent.TimeUnit

class QueryInfluxDbCommand extends HystrixCommand<List<QueryResult.Result>> {

   private final InfluxDB influxDb
   private final String database
   private final List<String> queryStatements

   QueryInfluxDbCommand(InfluxDB influxDb, String database, List<String> queryStatements) {
      super(HystrixCommand.Setter
         .withGroupKey(HystrixCommandGroupKey.Factory.asKey('InfluxDB'))
         .andCommandKey(HystrixCommandKey.Factory.asKey('Query'))
         .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey('InfluxDBQueries'))
         .andThreadPoolPropertiesDefaults(HystrixThreadPoolProperties.defaultSetter()
            .withCoreSize(2)
            .withMaxQueueSize(100)
            .withQueueSizeRejectionThreshold(10)
         )
         .andCommandPropertiesDefaults(HystrixCommandProperties.defaultSetter()
            .withExecutionTimeoutInMilliseconds(30000)
         )
      )
      this.influxDb = influxDb
      this.database = database
      this.queryStatements = queryStatements
   }

   @Override
   protected List<QueryResult.Result> run() throws Exception {
      def queryString = queryStatements.join('')
      def queryResult = influxDb.query(new Query(queryString, database), TimeUnit.MILLISECONDS)

      if (queryResult.hasError()) {
         throw new HystrixBadRequestException("global InfluxDB error: '${queryResult.error}' for query $queryString to DB $database")
      }

      return queryResult.results
   }
}
