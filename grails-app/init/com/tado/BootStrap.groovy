package com.tado

import com.google.common.base.Optional
import com.tado.hvaccontrol.TadoSystemType
import com.tado.influxdb.InfluxDbService
import com.tado.influxdb.TadoInfluxDb
import com.tado.zone.Zone
import grails.util.Environment
import org.joda.time.DateTimeZone
import org.joda.time.Instant

import static com.tado.hvaccontrol.TadoMode.AWAY
import static com.tado.hvaccontrol.TadoMode.HOME
import static com.tado.hvaccontrol.Temperature.fromCelsius
import static com.tado.hvaccontrol.TerminationCondition.Type.MANUAL
import static com.tado.hvaccontrol.UserHeatingSetting.off
import static com.tado.hvaccontrol.UserHeatingSetting.on
import static com.tado.zone.ZoneInfluxDbTimeSeries.OVERLAY_TERMINATION_TYPE
import static com.tado.zone.ZoneInfluxDbTimeSeries.SCHEDULE_USER_HEATING_SETTING
import static com.tado.zone.ZoneInfluxDbTimeSeries.TADO_MODE
import static com.tado.zone.ZoneInfluxDbTimeSeries.USER_HEATING_SETTING

class BootStrap {

    InfluxDbService influxDbService
    def grailsApplication
    TadoInfluxDb tadoInfluxDb

    def init = { servletContext ->
        if (Environment.current == Environment.TEST) {
            return
        }

        tadoInfluxDb = new TadoInfluxDb(influxDbService: influxDbService, grailsApplication: grailsApplication)
        tadoInfluxDb.clean()

        Home home = initHome()
        Zone zone = home.heatingZones.first()

        initZonePoints(zone)
    }

    def destroy = {
    }

    private Home initHome() {
        def homeTimeZone = DateTimeZone.forID("Europe/Berlin")
        def creationTimestamp = Instant.now().toDateTime(homeTimeZone).minusWeeks(1).toInstant()
        Home home = new Home(
                dateCreated: creationTimestamp,
                dateTimeZone: homeTimeZone
        )
        Zone zone = new Zone(
                dateCreated: creationTimestamp,
                discriminator: 1,
                type: TadoSystemType.HEATING
        )

        home.addToZones(zone)
        home.save(failOnError: true)

        tadoInfluxDb.writeZonePoint(zone, creationTimestamp, USER_HEATING_SETTING, on(fromCelsius(22)))
        tadoInfluxDb.writeZonePoint(zone, creationTimestamp, TADO_MODE, HOME)
        tadoInfluxDb.writeZonePoint(zone, creationTimestamp, OVERLAY_TERMINATION_TYPE, Optional.absent())

        return home
    }

    private initZonePoints(Zone zone) {
        use(TimeZoneCategory) {
            zone.yesterdayAt(0).with { Instant writeTime ->
                tadoInfluxDb.writeZonePoint(zone, writeTime, USER_HEATING_SETTING, off())
                tadoInfluxDb.writeZonePoint(zone, writeTime, SCHEDULE_USER_HEATING_SETTING, off())
                tadoInfluxDb.writeZonePoint(zone, writeTime, TADO_MODE, HOME)
                tadoInfluxDb.writeZonePoint(zone, writeTime, OVERLAY_TERMINATION_TYPE, Optional.absent())
            }
            zone.yesterdayAt(7).with { Instant writeTime ->
                tadoInfluxDb.writeZonePoint(zone, writeTime, USER_HEATING_SETTING, on(fromCelsius(22)))
                tadoInfluxDb.writeZonePoint(zone, writeTime, SCHEDULE_USER_HEATING_SETTING, on(fromCelsius(22)))
            }
            zone.yesterdayAt(9).with { Instant writeTime ->
                tadoInfluxDb.writeZonePoint(zone, writeTime, USER_HEATING_SETTING, on(fromCelsius(16)))
                tadoInfluxDb.writeZonePoint(zone, writeTime, TADO_MODE, AWAY)
            }
            zone.yesterdayAt(15).with { Instant writeTime ->
                tadoInfluxDb.writeZonePoint(zone, writeTime, USER_HEATING_SETTING, on(fromCelsius(22)))
                tadoInfluxDb.writeZonePoint(zone, writeTime, TADO_MODE, HOME)
            }
            zone.yesterdayAt(17).with { Instant writeTime ->
                tadoInfluxDb.writeZonePoint(zone, writeTime, USER_HEATING_SETTING, on(fromCelsius(19)))
                tadoInfluxDb.writeZonePoint(zone, writeTime, SCHEDULE_USER_HEATING_SETTING, on(fromCelsius(19)))
            }
            zone.yesterdayAt(19).with { Instant writeTime ->
                tadoInfluxDb.writeZonePoint(zone, writeTime, USER_HEATING_SETTING, on(fromCelsius(20)))
                tadoInfluxDb.writeZonePoint(zone, writeTime, OVERLAY_TERMINATION_TYPE, Optional.of(MANUAL))
            }
            zone.yesterdayAt(20).with { Instant writeTime ->
                tadoInfluxDb.writeZonePoint(zone, writeTime, USER_HEATING_SETTING, on(fromCelsius(19)))
                tadoInfluxDb.writeZonePoint(zone, writeTime, OVERLAY_TERMINATION_TYPE, Optional.absent())
            }
            zone.yesterdayAt(22).with { Instant writeTime ->
                tadoInfluxDb.writeZonePoint(zone, writeTime, USER_HEATING_SETTING, off())
                tadoInfluxDb.writeZonePoint(zone, writeTime, SCHEDULE_USER_HEATING_SETTING, off())
            }
        }
    }

    static class TimeZoneCategory {
        static Instant yesterdayAt(Zone self, int hour, int minute = 0) {
            return Instant.now().toDateTime(self.home.dateTimeZone)
                          .withTimeAtStartOfDay()
                          .minusDays(1)
                          .withHourOfDay(hour)
                          .withMinuteOfHour(minute)
                          .toInstant()
        }
    }
}
