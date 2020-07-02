package com.tado

import com.tado.hvaccontrol.TadoSystemType
import com.tado.zone.Zone
import org.joda.time.Instant

class BootStrap {

    def init = { servletContext ->
        def now = Instant.now()
        Home home = new Home(
                dateCreated: now
        )
        Zone zone = new Zone(
                dateCreated: now,
                discriminator: 1,
                type: TadoSystemType.HEATING
        )

        home.addToZones(zone)
        home.save(failOnError: true)
    }
    def destroy = {
    }
}
