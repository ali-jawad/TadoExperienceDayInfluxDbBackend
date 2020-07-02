package com.tado.influxdb


import com.netflix.hystrix.exception.HystrixRuntimeException
import okhttp3.ConnectionPool
import okhttp3.OkHttpClient
import org.influxdb.InfluxDB
import org.influxdb.InfluxDBFactory
import org.influxdb.dto.Point
import org.influxdb.dto.QueryResult
import rx.Single

import javax.annotation.PostConstruct
import java.util.concurrent.TimeUnit

class InfluxDbService {

   private final static String INFLUXDB_DATABASE_NAME = 'tado_product'
   final static String HOME_MEASUREMENT = 'home'

   def grailsApplication

   private InfluxDB influxDb

   @PostConstruct
   void init() {
      def config = grailsApplication.config.influxDb

      def builder = new OkHttpClient.Builder()
         .connectTimeout(500, TimeUnit.MILLISECONDS)
         .readTimeout(20, TimeUnit.SECONDS)
         .writeTimeout(5, TimeUnit.SECONDS)
         .retryOnConnectionFailure(true)
         .connectionPool(new ConnectionPool(20, 1, TimeUnit.MINUTES))

      influxDb = InfluxDBFactory.connect(config.url, config.username, config.password, builder)
   }

   void write(Point point, String retentionPolicy = null, Integer collapserMaxRequestsInBatch = 5000) {
      def command = new WriteInfluxDbCollapser(influxDb, INFLUXDB_DATABASE_NAME, point, collapserMaxRequestsInBatch, retentionPolicy)
      command.queue()
   }

   QueryResult.Result query(String singleStatement) {
      def results = queryStatements([singleStatement]).toBlocking().value()
      assert results.size() == 1: "expected one InfluxDB result, got ${results.size()}"
      return results.first()
   }

    InfluxDbResult query(InfluxDbRequest request) {
      return request.createResult(queryStatements(request.statements).toBlocking().value())
   }

   private Single<List<QueryResult.Result>> queryStatements(List<String> queryStatements) {
      assert queryStatements.every { it.endsWith(';') }

      try {
         def command = new QueryInfluxDbCommand(influxDb, INFLUXDB_DATABASE_NAME, queryStatements)
         return command.observe().toSingle().onErrorResumeNext({
            err -> Single.error(new RuntimeException(err))
         })
         // We still need to handle it here:
         // as stated in the AbstractCommand.observe javadoc, HystrixRuntimeException can be also thrown "immediately if the command can not be queued (such as short-circuited, thread-pool/semaphore rejected)"
      } catch (HystrixRuntimeException e) {
         return Single.error(new RuntimeException(e))
      }
   }

}
