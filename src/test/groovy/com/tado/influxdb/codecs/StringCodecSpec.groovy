package com.tado.influxdb.codecs

import com.tado.influxdb.InfluxDbPointBuilder
import com.tado.influxdb.InfluxDbRequest
import com.tado.influxdb.InfluxDbResult
import com.tado.influxdb.InfluxDbTimeSeries
import com.tado.influxdb.QueryInfluxDbCommand
import com.tado.influxdb.TimeSeriesKey
import com.tado.influxdb.queries.QueryBuilder
import com.tado.influxdb.queries.RawQuery
import org.influxdb.InfluxDB
import org.influxdb.InfluxDBFactory
import org.joda.time.Instant
import spock.lang.Shared
import spock.lang.Specification

import static org.joda.time.Duration.standardHours

class StringCodecSpec extends Specification {
    @Shared
    InfluxDB influxDb

    String database
    InfluxDbRequest request
    Instant timeZero
    int lowerBoundInHours = 24 * 7

    def setupSpec() {
        influxDb = InfluxDBFactory.connect('http://localhost:8087', 'u', 'p')
        influxDb.setLogLevel(InfluxDB.LogLevel.FULL)
    }

    def setup() {
        database = "influx_spec_${UUID.randomUUID().toString().replaceAll('-', '_')}"
        influxDb.createDatabase(database)
        timeZero = new Instant(1451606400000) // 01.01.2016
        def start = timeZero
        def end = timeZero + standardHours(24)
        def lowerBound = start - standardHours(lowerBoundInHours)
        request = new InfluxDbRequest(start, end, lowerBound)
    }

    def cleanup() {
        influxDb.deleteDatabase(database)
    }

    def "backslashes a encoded and decoded symmetrically"() {
        given:
        def ts = InfluxDbTimeSeries.timeSeries('f1', InfluxDbCodecs.stringCodec())
        def measurement = 'test'
        def tags = [tag1: '1']
        def pointBuilder = new InfluxDbPointBuilder(measurement, tags)
        pointBuilder.at(timeZero)
        pointBuilder.add(ts, value)
        influxDb.write(database, null, pointBuilder.build())

        when:
        RawQuery query = new QueryBuilder(
                key: new TimeSeriesKey(measurement, tags),
                timeSeries: ts)
                .raw()
        request.addQuery(query)

        InfluxDbResult result = execute(request)

        then:
        def dp = result.get(query).first()
        dp.value == value

        where:
        value             | _
        'test'            | _
        'test\\'          | _
        'test\\something' | _
        '\\\\\\\\\\'      | _
    }

    private InfluxDbResult execute(InfluxDbRequest request) {
        def results = new QueryInfluxDbCommand(influxDb, database, request.statements).execute()
        return request.createResult(results)
    }
}
