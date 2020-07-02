package com.tado

import com.tado.hvaccontrol.TadoSystemType
import com.tado.influxdb.InfluxDbService
import com.tado.zone.Zone
import org.joda.time.DateTimeZone
import org.joda.time.Instant

class Home {
    Instant dateCreated
    DateTimeZone dateTimeZone

    static mapping = {
        autoTimestamp false
    }

    static constraints = {
        dateCreated bindable: true
    }

    static hasMany = [
        zones: Zone
    ]

    Set<Zone> getHeatingZones() {
        zones.findAll { it.type == TadoSystemType.HEATING }
    }

    String getInfluxDbMeasurement() {
        return InfluxDbService.HOME_MEASUREMENT
    }

    Map<String, String> getInfluxDbTags() {
        return [home: String.valueOf(id)]
    }
}
