package com.tado.influxdb.codecs

class OptionalIntegerInfluxDbCodec<T> implements IntegerInfluxDbCodec<Optional<T>> {

   private final int absentValue

   private final InfluxDbCodec<T, Integer> codec

   OptionalIntegerInfluxDbCodec(InfluxDbCodec<T, Integer> codec, int absentValue) {
      this.codec = codec
      this.absentValue = absentValue
   }

   @Override
   Integer encode(Optional<T> opt) {
      return opt.present ? codec.encode(opt.get()) : absentValue
   }

   @Override
   Optional<T> decode(Integer value) {
      value == absentValue ? Optional.absent() : Optional.of(codec.decode(value))
   }

   @Override
   String toString() {
      "optional $codec"
   }
}
