package com.tado.hvaccontrol

import groovy.transform.CompileStatic

@CompileStatic
final class Temperature implements Serializable, Comparable<Temperature> {

   private double celsius

   // required for Hibernate
   private Temperature() {}

   private Temperature(double temperatureInCelsius) {
      this.celsius = temperatureInCelsius
   }

   public static double convertToCelsius(double temperatureInFahrenheit) {
      (temperatureInFahrenheit - 32.0) / 1.8000
   }

   public static double convertToFahrenheit(double temperatureInCelsius) {
      temperatureInCelsius * 1.8000 + 32
   }

   public static Temperature fromCelsius(double temperatureInCelsius) {
      withUnit(temperatureInCelsius, com.tado.hvaccontrol.Temperature.Unit.CELSIUS)
   }

   public static Temperature fromCentiCelsius(int temperatureInCentiCelsius) {
      withUnit(temperatureInCentiCelsius / 100.0 as double, com.tado.hvaccontrol.Temperature.Unit.CELSIUS)
   }

   public static Temperature fromFahrenheit(double temperatureInFahrenheit) {
      withUnit(temperatureInFahrenheit, com.tado.hvaccontrol.Temperature.Unit.FAHRENHEIT)
   }

   public static Temperature withUnit(double temperature, Unit unit) {
      switch(unit) {
         case com.tado.hvaccontrol.Temperature.Unit.CELSIUS:
            return new Temperature(temperature)
         case com.tado.hvaccontrol.Temperature.Unit.FAHRENHEIT:
            return new Temperature(convertToCelsius(temperature))
         default:
            throw new UnsupportedOperationException("unit $unit not supported")
      }
   }

   public Temperature plus(Temperature temperature) {
      return fromCelsius(inCelsius + temperature.inCelsius)
   }

   public Temperature minus(Temperature temperature) {
      return fromCelsius(inCelsius - temperature.inCelsius)
   }

   public double getInCelsius() {
      getIn(com.tado.hvaccontrol.Temperature.Unit.CELSIUS)
   }

   public double getInFahrenheit() {
      getIn(com.tado.hvaccontrol.Temperature.Unit.FAHRENHEIT)
   }

   public double getIn(Unit unit) {
      switch(unit) {
         case com.tado.hvaccontrol.Temperature.Unit.CELSIUS:
            return celsius
         case com.tado.hvaccontrol.Temperature.Unit.FAHRENHEIT:
            return convertToFahrenheit(celsius)
         default:
            throw new UnsupportedOperationException("unit $unit not supported")
      }
   }

   public int getCentiCelsius() {
      return (int) Math.round(inCelsius * 100)
   }

   /**
    * @param precision the number of decimal places. 0: integer rounding
    */
   public Temperature roundTo(Unit unit, int precision = 0) {
      Double inUnit = getIn(unit)
      return withUnit(inUnit.round(precision), unit)
   }

   @Override
   int compareTo(Temperature o) {
      return ((Double) this.celsius).compareTo(o.celsius)
   }

   static Temperature fromString(String temperature) {
      def m = temperature =~ /(.*) °C( \((.*) °F\))?/
      if (!m.matches()) {
         throw new IllegalArgumentException("cannot parse temperature '$temperature'")
      }
      if (m.group(3) != null) {
         return fromFahrenheit(Double.valueOf(m.group(3)))
      } else {
         return fromCelsius(Double.valueOf(m.group(1)))
      }
   }

   @Override
   String toString() {
      double celsiusRounded = ((Double) this.inCelsius).round(1)
      if (Math.round(this.inCelsius) == this.inCelsius) {
         "$celsiusRounded °C"
      } else if (Math.round(this.inFahrenheit) == this.inFahrenheit) {
         "$celsiusRounded °C (${this.inFahrenheit} °F)"
      } else {
         "$celsiusRounded °C"
      }
   }

   String toString(Unit unit) {
      return "${getIn(unit)} ${unit.symbol}"
   }

   public static enum Unit {
      CELSIUS, FAHRENHEIT

      public String getSymbol() {
         switch(this) {
            case CELSIUS:
               return '°C' // according to Wikipedia, degree character belongs to temp unit symbol (Kelvin does not have one)
            case FAHRENHEIT:
               return '°F' // according to Wikipedia, degree character belongs to temp unit symbol (Kelvin does not have one)
            default:
               throw new UnsupportedOperationException()
         }
      }
   }

   // TODO(florian): store celsius property as BigDecimal with X digits instead of using centiCelsius for comparison
   boolean equals(o) {
      if (this.is(o)) return true
      if (!(o instanceof Temperature)) return false

      Temperature that = (Temperature) o

      return that.centiCelsius == this.centiCelsius
   }

   // TODO(florian): store celsius property as BigDecimal with X digits instead of using centiCelsius for hashcode
   int hashCode() {
      def rounded = centiCelsius
      long temp = rounded != +0.0d ? Double.doubleToLongBits(rounded) : 0L
      return (int) (temp ^ (temp >>> 32))
   }
}
