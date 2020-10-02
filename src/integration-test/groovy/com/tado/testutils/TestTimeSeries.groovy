package com.tado.testutils

import com.tado.influxdb.InfluxDbTimeSeries
import com.tado.influxdb.TadoInfluxDb
import com.tado.zone.Zone
import grails.util.Holders
import org.joda.time.Instant
import org.junit.rules.ExternalResource

class TestTimeSeries extends ExternalResource {
    private TadoInfluxDb tadoInfluxDb

    public <T> void writeZonePoint(Zone zone, Instant reportingTime, InfluxDbTimeSeries<T, ?> timeSeries, T value) {
        tadoInfluxDb().writeZonePoint(zone, reportingTime, timeSeries, value)
    }

    @Override
    protected void after() {
        tadoInfluxDb().clean()
    }

    private TadoInfluxDb tadoInfluxDb() {
        if (!tadoInfluxDb) {
            tadoInfluxDb = new TadoInfluxDb(influxDbService: Holders.applicationContext.influxDbService, grailsApplication: Holders.grailsApplication)
        }
        return tadoInfluxDb
    }
}
