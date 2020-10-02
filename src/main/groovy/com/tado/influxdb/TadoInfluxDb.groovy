package com.tado.influxdb


import com.tado.zone.Zone
import okhttp3.ConnectionPool
import okhttp3.OkHttpClient
import org.influxdb.InfluxDB
import org.influxdb.InfluxDBFactory
import org.influxdb.dto.Query
import org.joda.time.Instant

import java.util.concurrent.TimeUnit

class TadoInfluxDb {
   private static final String DATABASE_NAME = "tado_product"

   InfluxDbService influxDbService
   def grailsApplication

   public <T> void writeZonePoint(Zone zone, Instant reportingTime, InfluxDbTimeSeries<T, ?> timeSeries, T value) {
      assert value != null: "value of $timeSeries is null when writing for zone $zone"

      def tags = [home: String.valueOf(zone.home.id), zone: String.valueOf(zone.discriminator)]
      def builder = new InfluxDbPointBuilder(InfluxDbService.HOME_MEASUREMENT, tags)
      builder.at(reportingTime)
      builder.add(timeSeries, value)
      influxDB().write(DATABASE_NAME, '', builder.build())
   }

   void clean() {
      influxDB().query(new Query("DELETE FROM \"home\"", DATABASE_NAME))
   }

   private InfluxDB influxDB() {
      def config = grailsApplication.config.influxDb

      def builder = new OkHttpClient.Builder()
              .connectTimeout(500, TimeUnit.MILLISECONDS)
              .readTimeout(20, TimeUnit.SECONDS)
              .writeTimeout(5, TimeUnit.SECONDS)
              .retryOnConnectionFailure(true)
              .connectionPool(new ConnectionPool(20, 1, TimeUnit.MINUTES))

      return InfluxDBFactory.connect(config.url, config.username, config.password, builder)
   }
}
