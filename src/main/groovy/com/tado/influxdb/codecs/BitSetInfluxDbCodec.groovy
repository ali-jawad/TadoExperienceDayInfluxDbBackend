package com.tado.influxdb.codecs

import com.google.common.base.Preconditions


class BitSetInfluxDbCodec implements IntegerInfluxDbCodec<BitSet> {

   @Override
   Integer encode(BitSet bitSet) {
      Preconditions.checkArgument(bitSet.length() <= 32, "bit set must not be longer than 32 bits" as Object)

      int result = 0

      for (int i = 0; i < bitSet.length(); i++) {
         if (bitSet.get(i))
            result |= (1 << i)
      }

      return result
   }

   @Override
   BitSet decode(Integer timeSeriesValue) {
      def bitSet = new BitSet(32)
      for (int i = 0; i < 32; i++) {
         if ((timeSeriesValue & (1 << i)) != 0)
            bitSet.set(i)
      }

      return bitSet
   }

}
