package com.tado.hvaccontrol

import com.tado.accd.setting.AcMode
import com.tado.accd.setting.AcSetting
import com.tado.accd.setting.FanSpeed
import com.tado.accd.setting.Swing

class UserAcSetting extends UserSetting {

   Power power
   AcMode mode
   Double temperatureInCelsius
   FanSpeed fanSpeed
   Swing swing

   static transients = ['temperature', 'valid']

   static constraints = {
      power nullable: true
      mode nullable: true, validator: { mode, setting -> setting.power == Power.ON || mode == null }
      temperatureInCelsius nullable: true, validator: { temperatureInCelsius, setting -> setting.power == Power.ON || temperatureInCelsius == null }
      fanSpeed nullable: true, validator: { fanSpeed, setting -> setting.power == Power.ON || fanSpeed == null }
      swing nullable: true, validator: { swing, setting -> setting.power == Power.ON || swing == null }
   }

   static mapping = {
      discriminator TadoSystemType.AIR_CONDITIONING.name()
   }

   static UserAcSetting off() {
      return new UserAcSetting(power: Power.OFF)
   }

   static UserAcSetting on(AcMode mode, Temperature temperature = null, FanSpeed fanSpeed = null, Swing swing = null) {
      return new UserAcSetting(power: Power.ON, mode: mode, temperatureInCelsius: temperature?.inCelsius, fanSpeed: fanSpeed, swing: swing)
   }

   static UserAcSetting cool(Temperature temperature, FanSpeed fanSpeed = null, Swing swing = null) {
      return new UserAcSetting(power: Power.ON, mode: AcMode.COOL, temperatureInCelsius: temperature.inCelsius, fanSpeed: fanSpeed, swing: swing)
   }

   @Override
   TadoSystemType getType() {
      return TadoSystemType.AIR_CONDITIONING
   }

   /**
    * @return a copy of this UserAcSetting with the swing set to the given one
    */
   public UserAcSetting withSwing(Swing newSwing) {
      new UserAcSetting(
         power: power,
         mode: mode,
         temperatureInCelsius: temperatureInCelsius,
         fanSpeed: fanSpeed,
         swing: newSwing
      )
   }


   @Override
   public Temperature getTemperature() {
      temperatureInCelsius == null ? null : Temperature.fromCelsius(temperatureInCelsius)
   }

   @Override
   public void setTemperature(Temperature temperature) {
      temperatureInCelsius = temperature?.inCelsius
   }

   public boolean includesMode() {
      mode != null
   }

   @Override
   public boolean includesTemperature() {
      temperature != null
   }

   public boolean includesFanSpeed() {
      fanSpeed != null
   }

   public boolean includesSwing() {
      swing != null
   }

   @Override
   public boolean isValid() {
      if (power == Power.OFF)
         return !includesMode() && !includesTemperature() && !includesFanSpeed() && !includesSwing()
      else
         return includesMode()
   }

   @Override
   UserAcSetting copy() {
      return new UserAcSetting(
         power: power,
         mode: mode,
         temperatureInCelsius: temperatureInCelsius,
         fanSpeed: fanSpeed,
         swing: swing
      )
   }

   public AcSetting convertToAcSetting(Temperature.Unit unit) {
      def roundedCelsiusTemp = temperature != null ? Math.round(temperature.getIn(unit)) : null

      return new AcSetting(
         power: power,
         mode: mode,
         temperature: roundedCelsiusTemp,
         temperatureUnit: unit.toAccdTemperatureUnit(),
         fanSpeed: fanSpeed,
         swing: swing
      )
   }

   @Override
   boolean isOn() {
      return this.power == Power.ON
   }

   boolean equals(o) {
      if (this.is(o)) return true
      if (!(o instanceof UserAcSetting)) return false

       UserAcSetting that = (UserAcSetting) o

      if (fanSpeed != that.fanSpeed) return false
      if (mode != that.mode) return false
      if (power != that.power) return false
      if (swing != that.swing) return false
      if (temperature != that.temperature) return false

      return true
   }

   int hashCode() {
      int result
      result = power.hashCode()
      result = 31 * result + (mode != null ? mode.hashCode() : 0)
      result = 31 * result + (temperature != null ? temperature.hashCode() : 0)
      result = 31 * result + (fanSpeed != null ? fanSpeed.hashCode() : 0)
      result = 31 * result + (swing != null ? swing.hashCode() : 0)
      return result
   }

   @Override
   String toString() {
      if (power == Power.OFF) {
         return "OFF"
      } else {
         String fanSpeed = includesFanSpeed() ? "${this.fanSpeed}" : "NOT SET"
         String swing = includesSwing() ? "${this.swing}" : "NOT SET"

         if (mode in [AcMode.COOL, AcMode.HEAT]) {
            String temp = includesTemperature() ? "$temperature" : "temp NOT SET"
            return "$mode ($temp), fan: $fanSpeed, swing: $swing"
         } else {
            return "$mode, fan: $fanSpeed, swing: $swing"
         }
      }
   }

   static UserAcSetting fromString(String userAcSetting) {
      if (userAcSetting == 'OFF') {
         return off()
      } else {
         def sm = userAcSetting =~ /(\w+).?(\((.*)\))?, fan: (.+), swing: (.+)/
         if (!sm.matches()) {
            throw new IllegalArgumentException("cannot parse user AC setting '$userAcSetting'")
         }
         return on(
            AcMode.valueOf(sm.group(1)),
            sm.group(3) in [null, 'temp NOT SET'] ? null : Temperature.fromString(sm.group(3)),
            sm.group(4) == 'NOT SET' ? null : FanSpeed.valueOf(sm.group(4)),
            sm.group(5) == 'NOT SET' ? null : Swing.valueOf(sm.group(5))
         )
      }
   }

}
