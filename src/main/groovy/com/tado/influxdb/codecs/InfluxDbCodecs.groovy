package com.tado.influxdb.codecs


import com.tado.hvaccontrol.OnOffSwitch
import com.tado.hvaccontrol.Percentage
import com.tado.hvaccontrol.Temperature
import com.tado.hvaccontrol.Voltage
import org.joda.time.Duration

class InfluxDbCodecs {

   static InfluxDbCodec<String, String> stringCodec() {
      return new IdentityInfluxDbCodec<String>()
   }

   static InfluxDbCodec<Double, Double> doubleCodec() {
      return new IdentityInfluxDbCodec<Double>()
   }

   static InfluxDbCodec<Long, Long> longCodec() {
      return new LongInfluxDbCodec<Long>() {
         @Override
         Long encode(Long domainObject) {
            return domainObject
         }

         @Override
         Long decode(Long value) {
            return value
         }
      }
   }

   static InfluxDbCodec<Integer, Integer> integerCodec() {
      // IntegerInfluxDbCodec is marker interface known by the series when decoding
      // we need one because the results from Influx are in JSON which uses doubles
      return new IntegerInfluxDbCodec<Integer>(){

         @Override
         Integer encode(Integer domainObject) {
            return domainObject
         }

         @Override
         Integer decode(Integer value) {
            return value
         }
      }
   }

   static InfluxDbCodec<Boolean, Boolean> booleanCodec() {
      return new IdentityInfluxDbCodec<Boolean>()
   }

   static <DomainType> InfluxDbCodec<DomainType, Boolean> staticBooleanMapping(Map<DomainType, Boolean> mapping) {
      return new StaticMappingBooleanCodec<DomainType>(mapping)
   }

   static <DomainType> InfluxDbCodec<DomainType, Integer> staticIntMapping(Map<DomainType, Integer> mapping) {
      return new StaticMappingIntCodec<DomainType>(mapping)
   }

   static InfluxDbCodec<Temperature, Integer> temperature() {
      return new TemperatureInfluxDbCodec()
   }

   static InfluxDbCodec<Voltage, Double> voltage() {
      return new VoltageInfluxDbCodec()
   }

   static <T> InfluxDbCodec<Optional<T>, Integer> optional(InfluxDbCodec<T, Integer> codec, int absentValue) {
      return new OptionalIntegerInfluxDbCodec(codec, absentValue)
   }

   static InfluxDbCodec<Percentage, Double> percentage() {
      return new PercentageInfluxDbCodec()
   }

   static <DomainType, InfluxDbType> InfluxDbCodec<Set<DomainType>, String> setCodec(InfluxDbCodec<DomainType, InfluxDbType> valueCodec) {
      return new SetInfluxDbCodec<DomainType>(valueCodec)
   }

   static InfluxDbCodec<BitSet, Integer> bitSetCodec() {
      return new BitSetInfluxDbCodec()
   }

   static InfluxDbCodec<Duration, Integer> durationCodec() {
      return new DurationCodec()
   }

   static InfluxDbCodec<OnOffSwitch, Integer> onOffSwitchCodec() {
      return staticIntMapping(ON_OFF_SWITCH)
   }

   static final ON_OFF_SWITCH = [
      (OnOffSwitch.OFF): 0,
      (OnOffSwitch.ON) : 1,
   ].asImmutable()
}
