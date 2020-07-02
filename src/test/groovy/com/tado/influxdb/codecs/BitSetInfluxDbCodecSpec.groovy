package com.tado.influxdb.codecs

import spock.lang.Specification


class BitSetInfluxDbCodecSpec extends Specification {

    def codec = new BitSetInfluxDbCodec()
    def bitSet = new BitSet()

    def "no bits set"() {
        expect:
        codec.decode(codec.encode(bitSet)) == bitSet
    }

    def "lowest bit set"() {
        given:
        bitSet.set(0)

        expect:
        codec.decode(codec.encode(bitSet)) == bitSet
    }


    def "highest bit set"() {
        given:
        bitSet.set(31)

        expect:
        codec.decode(codec.encode(bitSet)) == bitSet
    }

    def "lowest and highest bit set"() {
        given:
        bitSet.set(0)
        bitSet.set(31)

        expect:
        codec.decode(codec.encode(bitSet)) == bitSet
    }

    def "all bits set"() {
        given:
        bitSet.set(0, 32)

        expect:
        codec.decode(codec.encode(bitSet)) == bitSet
    }

    def "bit set too long"() {
        given:
        bitSet.set(0, 33)

        when:
        codec.encode(bitSet)

        then:
        thrown(IllegalArgumentException)
    }
}
