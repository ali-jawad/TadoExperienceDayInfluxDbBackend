package com.tado.testutils

import com.tado.Home
import com.tado.zone.Zone
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.junit.rules.ExternalResource

import static com.tado.hvaccontrol.TadoSystemType.HEATING

class TestHomes extends ExternalResource {
    private final Collection<Home> homes = []

    Home createHome(DateTimeZone timeZone) {
        Home.withTransaction {
            def createdAt = DateTime.now().minusWeeks(1).toInstant()
            Home home = new Home(dateCreated: createdAt, dateTimeZone: timeZone)
            Zone zone = new Zone(dateCreated: createdAt, type: HEATING, discriminator: 1)
            home.addToZones(zone)
            home.save(failOnError: true)
            return home
        }
    }

    @Override
    protected void after() {
        homes.each { home ->
            Home.withTransaction { home.delete() }
        }
    }
}
