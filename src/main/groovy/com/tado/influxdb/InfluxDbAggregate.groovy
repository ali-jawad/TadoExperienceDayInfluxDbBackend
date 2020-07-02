package com.tado.influxdb

interface InfluxDbAggregate {

   String getInfluxDbMeasurement()

   Map<String, String> getInfluxDbTags()

}
