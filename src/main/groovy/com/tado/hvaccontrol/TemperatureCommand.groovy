package com.tado.hvaccontrol

import grails.validation.Validateable

class TemperatureCommand {
   @Delegate(interfaces = false) Json json

   TemperatureCommand(map) {
      this.json = new Json(map as Map)
   }

   @Lazy Double celsius = json.celsius ? Double.parseDouble(json.celsius) : null
   @Lazy Double fahrenheit = json.fahrenheit ? Double.parseDouble(json.fahrenheit) : null

   static class Json implements Validateable {
      String celsius
      String fahrenheit

      static constraints = {
         celsius nullable: true, validator: { String celsius, TemperatureCommand.Json command ->
            if(celsius != null && !celsius.isNumber()) {
               return 'celsiusNotANumber'
            }
            if(celsius == null && command.fahrenheit == null) {
               return 'celsius.or.fahrenheit'
            }
            return true
         }
         fahrenheit nullable: true, validator: { String fahrenheit, TemperatureCommand.Json command ->
            if(fahrenheit != null && !fahrenheit.isNumber()) {
               return 'fahrenheitNotANumber'
            }
            return true
         }
      }
   }

   Temperature getTemperature(Temperature.Unit tempUnitWhenCelsiusAndFahrenheit = Temperature.Unit.CELSIUS) {
      assert celsius != null || fahrenheit != null
      if(celsius != null && fahrenheit != null) {
         switch (tempUnitWhenCelsiusAndFahrenheit) {
            case Temperature.Unit.CELSIUS:
               return Temperature.fromCelsius(celsius)
            case Temperature.Unit.FAHRENHEIT:
               return Temperature.fromFahrenheit(fahrenheit)
            default:
               throw new UnsupportedOperationException("temperature unit $tempUnitWhenCelsiusAndFahrenheit not supported")
         }
      }
      else if(celsius != null)
         return Temperature.fromCelsius(celsius)
      else
         return Temperature.fromFahrenheit(fahrenheit)
   }

}
