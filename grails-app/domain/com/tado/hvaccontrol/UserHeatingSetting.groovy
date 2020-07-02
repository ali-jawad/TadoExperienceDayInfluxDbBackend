package com.tado.hvaccontrol

class UserHeatingSetting extends UserSetting {

   public final static Temperature OFF_EQUIVALENT_TEMPERATURE = Temperature.fromCelsius(5)

    Power power
   Double temperatureInCelsius

   static transients = ['temperature', 'valid']

   static constraints = {
      temperatureInCelsius nullable: true, scale: 2,
         validator: { temperatureInCelsius, setting ->
            if (setting.power == Power.ON) {
               return (temperatureInCelsius != null)
            } else {
               return (temperatureInCelsius == null)
            }
         }
   }

   static mapping = {
      discriminator TadoSystemType.HEATING.name()
   }

   public static UserHeatingSetting off() {
      return new UserHeatingSetting(power: Power.OFF)
   }

   public static UserHeatingSetting on(Temperature temperature) {
      return new UserHeatingSetting(power: Power.ON, temperatureInCelsius: temperature.inCelsius)
   }

   @Override
   TadoSystemType getType() {
      return TadoSystemType.HEATING
   }

   @Override
   public Temperature getTemperature() {
      temperatureInCelsius == null ? null : Temperature.fromCelsius(temperatureInCelsius)
   }

   @Override
   public void setTemperature(Temperature temperature) {
      temperatureInCelsius = temperature?.inCelsius
   }

    Temperature getEffectiveTemperature() {
      power == Power.OFF ? OFF_EQUIVALENT_TEMPERATURE : temperature
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
   UserHeatingSetting copy() {
      return new UserHeatingSetting(
         power: power,
         temperatureInCelsius: temperatureInCelsius
      )
   }

   boolean equals(o) {
      if (this.is(o)) return true
      if (!(o instanceof UserHeatingSetting)) return false

       UserHeatingSetting that = (UserHeatingSetting) o

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
   String toString() {
      if (power == Power.OFF) {
         return "OFF"
      } else {
         return "ON ($temperature)"
      }
   }

   static UserHeatingSetting fromString(String userHeatingSetting) {
      if (userHeatingSetting == 'OFF') {
         return off()
      } else {
         def m = userHeatingSetting =~ /ON \((.*)\)/
         if (!m.matches()) {
            throw new IllegalArgumentException("cannot parse user heating setting '$userHeatingSetting'")
         }
         return on(Temperature.fromString(m.group(1)))
      }
   }
}
