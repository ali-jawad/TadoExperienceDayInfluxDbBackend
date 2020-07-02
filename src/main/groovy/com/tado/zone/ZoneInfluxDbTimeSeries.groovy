package com.tado.zone

import com.google.common.base.Optional
import com.tado.hvaccontrol.Power
import com.tado.hvaccontrol.TadoMode
import com.tado.hvaccontrol.Temperature
import com.tado.hvaccontrol.TerminationCondition
import com.tado.hvaccontrol.UserHeatingSetting
import com.tado.influxdb.InfluxDbTimeSeries
import com.tado.influxdb.codecs.InfluxDbCodecs
import com.tado.influxdb.codecs.MapInfluxDbCodec
import groovy.transform.CompileStatic

@CompileStatic
class ZoneInfluxDbTimeSeries {

   static final InfluxDbTimeSeries<TadoMode, Integer> TADO_MODE =
           InfluxDbTimeSeries.timeSeries('tadoMode', InfluxDbCodecs.staticIntMapping(Mappings.TADO_MODE_MAPPING))

   static final InfluxDbTimeSeries<Optional<TerminationCondition.Type>, Integer> OVERLAY_TERMINATION_TYPE =
           InfluxDbTimeSeries.timeSeries('overlayTerminationType', InfluxDbCodecs.staticIntMapping(Mappings.OVERLAY_TERMINATION_TYPE_MAPPING))

   static final InfluxDbTimeSeries<UserHeatingSetting, Map<String, ?>> USER_HEATING_SETTING =
           createUserHeatingSettingTimeseries('setting')

    static final InfluxDbTimeSeries<UserHeatingSetting, Map<String, ?>> SCHEDULE_USER_HEATING_SETTING =
            createUserHeatingSettingTimeseries('scheduleSetting')

   private static InfluxDbTimeSeries<UserHeatingSetting, Map<String, ?>> createUserHeatingSettingTimeseries(String timeseriesName) {
      return InfluxDbTimeSeries.timeSeries(timeseriesName, MapInfluxDbCodec.<UserHeatingSetting>mapCodec([
              InfluxDbTimeSeries.timeSeries('power', InfluxDbCodecs.staticBooleanMapping(Mappings.HEATING_POWER_MAPPING)),
              InfluxDbTimeSeries.timeSeries('temperature', InfluxDbCodecs.temperature())
      ],
         { UserHeatingSetting setting ->
            def value = [:]
            value['power'] = setting.power
            if (setting.includesTemperature()) {
               value['temperature'] = setting.temperature
            }
            return value as Map<String, ?>
         },
         { Map<String, ?> map ->
            def power = map.power as Power
            return new UserHeatingSetting(
               power: power,
               temperature: power == Power.ON ? map.temperature as Temperature : null
            )
         })
      )
   }

   static class Mappings {

      static final Map<TadoMode, Integer> TADO_MODE_MAPPING = [
         (TadoMode.HOME) : 0,
         (TadoMode.SLEEP): 1,
         (TadoMode.AWAY) : 2
      ].asImmutable()

      static final Map<Optional<TerminationCondition.Type>, Integer> OVERLAY_TERMINATION_TYPE_MAPPING = [
         (Optional.<TerminationCondition.Type> absent())         : 0,
         (Optional.of(TerminationCondition.Type.TADO_MODE))      : 1,
         (Optional.of(TerminationCondition.Type.MANUAL))         : 2,
         (Optional.of(TerminationCondition.Type.TIMER))          : 3,
         (Optional.of(TerminationCondition.Type.NEXT_TIME_BLOCK)): 4
      ].asImmutable()

      static final Map<Power, Boolean> HEATING_POWER_MAPPING = [
         (Power.ON) : true,
         (Power.OFF): false
      ].asImmutable()
   }
}
