package com.tado.hvaccontrol

final class TemperatureDifference implements Serializable, Comparable<TemperatureDifference> {

   private double celsius

   // required for Hibernate
   private TemperatureDifference() {}

   private TemperatureDifference(double temperatureInCelsius) {
      this.celsius = temperatureInCelsius
   }

   public static double convertToCelsius(double temperatureInFahrenheit) {
      temperatureInFahrenheit / 1.8000
   }

   public static double convertToFahrenheit(double temperatureInCelsius) {
      temperatureInCelsius * 1.8000
   }

   public static TemperatureDifference fromCelsius(double temperatureInCelsius) {
      withUnit(temperatureInCelsius, com.tado.hvaccontrol.Temperature.Unit.CELSIUS)
   }

   public static TemperatureDifference fromCentiCelsius(int temperatureInCentiCelsius) {
      withUnit(temperatureInCentiCelsius / 100.0, com.tado.hvaccontrol.Temperature.Unit.CELSIUS)
   }

   public static TemperatureDifference fromFahrenheit(double temperatureInFahrenheit) {
      withUnit(temperatureInFahrenheit, com.tado.hvaccontrol.Temperature.Unit.FAHRENHEIT)
   }

   public static TemperatureDifference withUnit(double temperature, Temperature.Unit unit) {
      switch(unit) {
         case com.tado.hvaccontrol.Temperature.Unit.CELSIUS:
            return new TemperatureDifference(temperature)
         case com.tado.hvaccontrol.Temperature.Unit.FAHRENHEIT:
            return new TemperatureDifference(convertToCelsius(temperature))
         default:
            throw new UnsupportedOperationException("unit $unit not supported")
      }
   }

   public TemperatureDifference plus(TemperatureDifference temperature) {
      return fromCelsius(inCelsius + temperature.inCelsius)
   }

   public TemperatureDifference minus(TemperatureDifference temperature) {
      return fromCelsius(inCelsius - temperature.inCelsius)
   }

   public double getInCelsius() {
      getIn(com.tado.hvaccontrol.Temperature.Unit.CELSIUS)
   }

   public double getInFahrenheit() {
      getIn(com.tado.hvaccontrol.Temperature.Unit.FAHRENHEIT)
   }

   public double getIn(Temperature.Unit unit) {
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
   public TemperatureDifference roundTo(Temperature.Unit unit, int precision = 0) {
      Double inUnit = getIn(unit)
      return withUnit(inUnit.round(precision), unit)
   }

   @Override
   int compareTo(TemperatureDifference o) {
      return ((Double) this.celsius).compareTo(o.celsius)
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

   String toString(Temperature.Unit unit) {
      return "${getIn(unit)} ${unit.symbol}"
   }

   // TODO(florian): store celsius property as BigDecimal with X digits instead of using centiCelsius for comparison
   boolean equals(o) {
      if (this.is(o)) return true
      if (!(o instanceof TemperatureDifference)) return false

      TemperatureDifference that = (TemperatureDifference) o

      return that.centiCelsius == this.centiCelsius
   }

   // TODO(florian): store celsius property as BigDecimal with X digits instead of using centiCelsius for hashcode
   int hashCode() {
      def rounded = centiCelsius
      long temp = rounded != +0.0d ? Double.doubleToLongBits(rounded) : 0L
      return (int) (temp ^ (temp >>> 32))
   }
}
