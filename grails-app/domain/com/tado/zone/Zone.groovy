package com.tado.zone

import com.tado.Home
import com.tado.hvaccontrol.TadoSystemType
import com.tado.influxdb.InfluxDbService
import org.joda.time.Instant

class Zone {
    Instant dateCreated
    TadoSystemType type
    int discriminator

    static belongsTo = [home: Home]

    static mapping = {
        autoTimestamp false
    }

    static constraints = {
        dateCreated bindable: true
    }

    String getInfluxDbMeasurement() {
        return InfluxDbService.HOME_MEASUREMENT
    }

    Map<String, String> getInfluxDbTags() {
        return home.influxDbTags + [zone: String.valueOf(discriminator)]
    }
}
