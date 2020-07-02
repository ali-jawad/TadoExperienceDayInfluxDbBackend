package com.tado.influxdb

import com.netflix.hystrix.HystrixCommand
import com.netflix.hystrix.HystrixCommandGroupKey
import com.netflix.hystrix.HystrixCommandKey
import com.netflix.hystrix.HystrixCommandProperties
import com.netflix.hystrix.HystrixThreadPoolKey
import com.netflix.hystrix.HystrixThreadPoolProperties
import groovy.util.logging.Slf4j
import org.influxdb.InfluxDB
import org.influxdb.dto.BatchPoints
import org.influxdb.dto.Point

@Slf4j
class WriteInfluxDbPointsCommand extends HystrixCommand<Void> {

   private final InfluxDB influxDb
   private final String database
   private final Collection<Point> points
   private final String retentionPolicy

   WriteInfluxDbPointsCommand(InfluxDB influxDb, String database, Collection<Point> points, String retentionPolicy) {
      super(HystrixCommand.Setter
         .withGroupKey(HystrixCommandGroupKey.Factory.asKey('InfluxDB'))
         .andCommandKey(HystrixCommandKey.Factory.asKey('WritePoints'))
         .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey('InfluxDBWriter'))
         .andThreadPoolPropertiesDefaults(HystrixThreadPoolProperties.defaultSetter()
            .withCoreSize(3)
            .withMaxQueueSize(1000)
            .withQueueSizeRejectionThreshold(100)
         )
         .andCommandPropertiesDefaults(HystrixCommandProperties.defaultSetter()
            .withExecutionTimeoutEnabled(false)
         // Reason: When Hystrix decides a command timed out, it interrupts the thread executing the command (see
         // https://github.com/Netflix/Hystrix/wiki/Configuration#execution.isolation.thread.interruptOnTimeout)).
         // OkHttpClient (and other HTTP clients for Java) do not respect the interrupted flag (see
         // http://stackoverflow.com/questions/3590000/what-does-java-lang-thread-interrupt-do) of the thread (see the
         // 'caveat' here: http://stackoverflow.com/a/4426050/1511832). Thus, the HTTP request just proceeds but the
         // invoker of the command assumes the command did not succeed. We currently e.g. log on WARN level about the
         // writing to be unsuccessful. Furthermore, the Hystrix metrics we ship to DataDog reflect this timeout error.
         //
         // Instead what we will rely on for timing out the writes is the socket timeouts set on the OkHttpClient of
         // the InfluxDb instance. When the sockets timeout happen, the HTTP client throws an exception. Any exception
         // thrown by a command is interpreted as a `FAIL` event by Hystrix. InfluxDB writes can still take longer than
         // the socket timeouts if e.g. the response 'trickles in', with parts of it arriving within the socket read
         // timeout.
         //
         // Slow HTTP requests will result in the thread pool being saturated quickly which will lead to new commands
         // being rejected and failing. The combination of command failures (due to socket timeouts) and threadpool
         // rejections should be enough to isolate InfluxDB from TGA. Furthermore, the only users of the write command
         // do not block for the write to finish anyway so that them failing more quickly (due to the Hystrix timeout)
         // is not necessary.
         )
      )

      this.influxDb = influxDb
      this.database = database
      this.points = points
      this.retentionPolicy = retentionPolicy
   }

   @Override
   protected Void run() throws Exception {
      try {
         influxDb.write(buildPointsBatch())
      } catch (Exception e) {
         log.error("error writing batch to Influx (${e.class}: ${e.message})")
         throw e
      }
      return null
   }

   @Override
   protected Void getFallback() {
   }

   private BatchPoints buildPointsBatch() {
      BatchPoints.Builder batchBuilder = BatchPoints
         .database(database)
         .consistency(InfluxDB.ConsistencyLevel.ONE)
         .retentionPolicy(retentionPolicy)
      points.each {
         batchBuilder.point(it)
      }
      return batchBuilder.build()
   }


   @Override
   String toString() {
      return "writing $points to database '$database' of InfluxDB"
   }
}
