package com.tado.hvaccontrol

class UserHotWaterSetting extends UserSetting {

   Power power
   Double temperatureInCelsius

   static transients = ['temperature', 'valid']

   static constraints = {
      temperatureInCelsius nullable: true, scale: 2,
         validator: { temperatureInCelsius, setting -> setting.power == Power.ON || temperatureInCelsius == null }
   }

   static mapping = {
      discriminator TadoSystemType.HOT_WATER.name()
   }

   public static UserHotWaterSetting off() {
      return new UserHotWaterSetting(power: Power.OFF)
   }

   public static UserHotWaterSetting on() {
      return new UserHotWaterSetting(power: Power.ON)
   }

   public static UserHotWaterSetting on(Temperature temperature) {
      return new UserHotWaterSetting(power: Power.ON, temperatureInCelsius: temperature.inCelsius)
   }

   @Override
   TadoSystemType getType() {
      return TadoSystemType.HOT_WATER
   }

   @Override
   public Temperature getTemperature() {
      temperatureInCelsius == null ? null : Temperature.fromCelsius(temperatureInCelsius)
   }

   @Override
   public void setTemperature(Temperature temperature) {
      temperatureInCelsius = temperature?.inCelsius
   }

   @Override
   public boolean includesTemperature() {
      temperature != null
   }

   @Override
   public boolean isValid() {
      if (power == Power.OFF)
         return !includesTemperature()
      else
         return true
   }

   @Override
   boolean isOn() {
      return this.power == Power.ON
   }

   @Override
   UserHotWaterSetting copy() {
      return new UserHotWaterSetting(
         power: power,
         temperatureInCelsius: temperatureInCelsius
      )
   }

   boolean equals(o) {
      if (this.is(o)) return true
      if (!(o instanceof UserHotWaterSetting)) return false

       UserHotWaterSetting that = (UserHotWaterSetting) o

      if (power != that.power) return false
      if (temperature != that.temperature) return false

      return true
   }

   int hashCode() {
      int result
      result = power.hashCode()
      result = 31 * result + (temperature != null ? temperature.hashCode() : 0)
      return result
   }

   @Override
   public String toString() {
      if(power == Power.OFF) {
         return "OFF"
      } else {
         String temp = includesTemperature() ? "$temperature" : "temp NOT SET"
         return "ON ($temp)"
      }
   }

   static UserHotWaterSetting fromString(String userHotWaterSetting) {
      if (userHotWaterSetting == 'OFF') {
         return off()
      } else {
         def m = userHotWaterSetting =~ /ON \((.*)\)/
         if (!m.matches()) {
            throw new IllegalArgumentException("cannot parse user heating setting '$userHotWaterSetting'")
         }
         if (m.group(1) == 'temp NOT SET') {
            return on()
         } else {
            return on(Temperature.fromString(m.group(1)))
         }
      }
   }

}
