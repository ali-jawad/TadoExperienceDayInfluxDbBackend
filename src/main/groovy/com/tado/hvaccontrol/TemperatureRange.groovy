package com.tado.hvaccontrol

class TemperatureRange {

   Map<Temperature.Unit, IntRange> ranges

   static TemperatureRange create(Map<Temperature.Unit, IntRange> ranges) {
      assert ranges != null
      return new TemperatureRange(ranges: ranges)
   }

   static TemperatureRange create(int min, int max, Temperature.Unit unit) {
      assert min < max
      Temperature.Unit secondaryUnit = unit == com.tado.hvaccontrol.Temperature.Unit.CELSIUS ? com.tado.hvaccontrol.Temperature.Unit.FAHRENHEIT : com.tado.hvaccontrol.Temperature.Unit.CELSIUS

      def perUnit = [:]

      perUnit[unit] = new IntRange(min, max)

      perUnit[secondaryUnit] = new IntRange(
         Temperature.withUnit(min as double, unit).roundTo(secondaryUnit).getIn(secondaryUnit) as int,
         Temperature.withUnit(max as double, unit).roundTo(secondaryUnit).getIn(secondaryUnit) as int
      )

      return create(perUnit)
   }

   private TemperatureRange() {}

   boolean includes(Temperature temperature, Temperature.Unit usedUnit) {
      return ranges[usedUnit].fromInt <= temperature.getIn(usedUnit) && temperature.getIn(usedUnit) <= ranges[usedUnit].toInt
   }

   Temperature adjust(Temperature temp, Temperature.Unit usedUnit) {
      if (temp.getIn(usedUnit) < (ranges[usedUnit].fromInt as double)) {
         return Temperature.withUnit(ranges[usedUnit].fromInt, usedUnit)
      } else if ((ranges[usedUnit].toInt as double) < temp.getIn(usedUnit)) {
         return Temperature.withUnit(ranges[usedUnit].toInt, usedUnit)
      } else {
         return temp
      }
   }

    IntRange getRange(Temperature.Unit unit) {
      return ranges[unit]
   }

   @Override
   String toString() {
      return "[${ranges[com.tado.hvaccontrol.Temperature.Unit.CELSIUS].from}–${ranges[com.tado.hvaccontrol.Temperature.Unit.CELSIUS].to}°C, ${ranges[com.tado.hvaccontrol.Temperature.Unit.FAHRENHEIT].from}–${ranges[com.tado.hvaccontrol.Temperature.Unit.FAHRENHEIT].to}°F]"
   }
}
