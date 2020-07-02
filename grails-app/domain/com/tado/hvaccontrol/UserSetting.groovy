package com.tado.hvaccontrol

abstract class UserSetting {

   static mapping = {
      version false
      discriminator column: "type"
   }

   static transients = ['temperature', 'type']

   abstract TadoSystemType getType()

   abstract boolean isValid()

   abstract UserSetting copy()

   abstract boolean includesTemperature()

   abstract boolean isOn()

   /**
    * @return null if this setting does not include a temperature
    */
   Temperature getTemperature() {
      return temperature
   }

   void setTemperature(Temperature temperature) {
      this.temperature = temperature
   }

   /**
    * @param precision the number of decimal places. 0: integer rounding
    */
   final void roundTemperature(Temperature.Unit unit, int precision) {
      if (includesTemperature()) {
         temperature = temperature.roundTo(unit, precision)
      }
   }
}
