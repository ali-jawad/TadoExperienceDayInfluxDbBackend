package com.tado.influxdb

import com.tado.influxdb.codecs.IdentityInfluxDbCodec
import com.tado.influxdb.codecs.InfluxDbCodecs
import com.tado.influxdb.codecs.MapInfluxDbCodec
import com.tado.influxdb.queries.DiscreteStateQuery
import com.tado.influxdb.queries.GroupedQuery
import com.tado.influxdb.queries.QueryBuilder
import com.tado.influxdb.queries.RawQuery
import org.influxdb.InfluxDB
import org.influxdb.InfluxDBFactory
import org.influxdb.dto.BatchPoints
import org.influxdb.dto.Point
import org.joda.time.Instant
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static org.joda.time.Duration.standardHours

class InfluxDbQuerySpec extends Specification {

    @Shared
    InfluxDB influxDb

    @Shared
    def intCodec = InfluxDbCodecs.integerCodec()

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

    private void write(String measurement, Map<String, Object> tags, List<Map<String, Object>> dps) {
        BatchPoints.Builder builder = BatchPoints.database(database)
        dps.each {
            def pointBuilder = Point.measurement(measurement)
                    .tag(tags.collectEntries { [it.key, it.value.toString()] } as Map<String, String>)
                    .time(timeZero.millis + standardHours(1).millis * it['t'], TimeUnit.MILLISECONDS)
            it.each { String key, Object value ->
                if (key == 't')
                    return
                pointBuilder.addField(key, value)
            }
            builder.point(pointBuilder.build())
        }

        influxDb.write(builder.build())
    }

    private InfluxDbResult execute(InfluxDbRequest request) {
        def results = new QueryInfluxDbCommand(influxDb, database, request.statements).execute()
        return request.createResult(results)
    }

    def "discrete TS (LAST() value, values in interval)"() {
        given:
        def measurement = 'test'
        def tags = [tag1: 1]
        def dps = [[t: -lowerBoundInHours + 1, f1: 1], [t: 1, f1: 2], [t: 8, f1: 3]]
        write(measurement, tags, dps)

        when:
        DiscreteStateQuery query = new QueryBuilder(
                key: new TimeSeriesKey(measurement, [tag1: 1]),
                timeSeries: InfluxDbTimeSeries.timeSeries('f1', intCodec))
                .discrete()
        request.addQuery(query)

        InfluxDbResult result = execute(request)

        then:
        def inHours = result.get(query).toUnitsAtStart(standardHours(1), timeZero)

        inHours.size() == 3
        inHours[0] == 1
        inHours[1] == 2
        inHours[8] == 3
    }

    def "raw TS (values in interval)"() {
        given:
        def measurement = 'test'
        def tags = [tag1: 1]
        def dps = [[t: -lowerBoundInHours + 1, f1: 1], [t: 1, f1: 2], [t: 8, f1: 3]]
        write(measurement, tags, dps)

        when:
        RawQuery query = new QueryBuilder(
                key: new TimeSeriesKey(measurement, [tag1: 1]),
                timeSeries: InfluxDbTimeSeries.timeSeries('f1', intCodec))
                .raw()
        request.addQuery(query)

        InfluxDbResult result = execute(request)

        then:
        def inHours = result.get(query).toUnits(standardHours(1), timeZero)

        inHours.size() == 2
        inHours[1] == 2
        inHours[8] == 3
    }

    def "raw TS (no values in interval)"() {
        given:
        def measurement = 'test'
        def tags = [tag1: 1]
        def dps = [[t: -lowerBoundInHours + 1, f1: 1]]
        write(measurement, tags, dps)

        when:
        RawQuery query = new QueryBuilder(
                key: new TimeSeriesKey(measurement, [tag1: 1]),
                timeSeries: InfluxDbTimeSeries.timeSeries('f1', intCodec))
                .raw()
        request.addQuery(query)

        InfluxDbResult result = execute(request)

        then:
        def points = result.get(query)
        points.isEmpty()
    }

    def "raw TS (no series in InfluxDB)"() {
        when:
        RawQuery query = new QueryBuilder(
                key: new TimeSeriesKey('non_existing', [tag1: 1]),
                timeSeries: InfluxDbTimeSeries.timeSeries('f1', intCodec))
                .raw()
        request.addQuery(query)
        def result = execute(request)

        then:
        result.get(query).isEmpty()
    }

    def "long value writing and reading"() {
        given:
        def measurement = 'test'
        def tags = [tag1: 1]
        def dps = [[t: 1, f1: Long.MAX_VALUE]]
        write(measurement, tags, dps)

        when:
        RawQuery query = new QueryBuilder(
                key: new TimeSeriesKey(measurement, [tag1: 1]),
                timeSeries: InfluxDbTimeSeries.timeSeries('f1', InfluxDbCodecs.longCodec()))
                .raw()
        request.addQuery(query)

        InfluxDbResult result = execute(request)

        then:
        def inHours = result.get(query).toUnits(standardHours(1), timeZero)

        inHours.size() == 1
        inHours[1] == Long.MAX_VALUE
    }

    def "two queries for discrete TS"() {
        given:
        def measurement = 'test'
        def tags = [tag1: 1]
        def dps = [
                [t: -lowerBoundInHours + 1, f1: 1, f2: 'a'],
                [t: 1, f1: 2],
                [t: 5, f1: 5, f2: 'b'],
                [t: 8, f2: 'c'],
                [t: 12, f1: 10, f2: 'a'],
        ]
        write(measurement, tags, dps)

        when:
        DiscreteStateQuery queryF1 = new QueryBuilder(
                key: new TimeSeriesKey(measurement, [tag1: 1]),
                timeSeries: InfluxDbTimeSeries.timeSeries('f1', intCodec))
                .discrete()
        request.addQuery(queryF1)

        DiscreteStateQuery queryF2 = new QueryBuilder(
                key: new TimeSeriesKey(measurement, [tag1: 1]),
                timeSeries: InfluxDbTimeSeries.timeSeries('f2', new IdentityInfluxDbCodec<String>()))
                .discrete()
        request.addQuery(queryF2)

        InfluxDbResult result = execute(request)

        then:
        def inHoursF1 = result.get(queryF1).toUnitsAtStart(standardHours(1), timeZero)

        inHoursF1.size() == 4
        inHoursF1[0] == 1
        inHoursF1[1] == 2
        inHoursF1[5] == 5
        inHoursF1[12] == 10

        and:
        def inHoursF2 = result.get(queryF2).toUnitsAtStart(standardHours(1), timeZero)

        inHoursF2.size() == 4
        inHoursF2[0] == 'a'
        inHoursF2[5] == 'b'
        inHoursF2[8] == 'c'
        inHoursF2[12] == 'a'
    }

    def "discrete TS with multiple fields"() {
        given:
        def measurement = 'test'
        def tags = [tag1: 1]
        def dps = [[t: -lowerBoundInHours + 1, 's.f1': 1, 's.f2': 10], [t: 5, 's.f1': 2, 's.f2': 20], [t: 8, 's.f1': 3]]
        write(measurement, tags, dps)

        when:
        DiscreteStateQuery query = new QueryBuilder(
                key: new TimeSeriesKey(measurement, [tag1: 1]),
                timeSeries: InfluxDbTimeSeries.timeSeries('s', MapInfluxDbCodec.mapCodec([InfluxDbTimeSeries.timeSeries('f1', intCodec), InfluxDbTimeSeries.timeSeries('f2', intCodec)],
                        { int[] a -> ['f1': a[0], 'f2': a[1]] }, { Map<String, Integer> m -> [m['f1'], m['f2']] })))
                .discrete()
        request.addQuery(query)

        InfluxDbResult result = execute(request)

        then:
        def inHours = result.get(query).toUnitsAtStart(standardHours(1), timeZero)

        inHours.size() == 3
        inHours[0] == [1, 10]
        inHours[5] == [2, 20]
        inHours[8] == [3, null]
    }

    def "discrete TS (no LAST() value at all)"() {
        given:
        def measurement = 'test'
        def tags = [tag1: 1]
        def dps = [[t: 5, f1: 1]]
        write(measurement, tags, dps)

        when:
        DiscreteStateQuery query = new QueryBuilder(
                key: new TimeSeriesKey(measurement, [tag1: 1]),
                timeSeries: InfluxDbTimeSeries.timeSeries('f1', intCodec))
                .discrete()
        request.addQuery(query)

        InfluxDbResult result = execute(request)

        then:
        def inHours = result.get(query).toUnitsAtStart(standardHours(1), timeZero)

        inHours.size() == 2
        inHours[0] == null
        inHours[5] == 1
    }

    def "discrete TS (no LAST() value since lower bound too high)"() {
        given:
        def measurement = 'test'
        def tags = [tag1: 1]
        def dps = [[t: -lowerBoundInHours - 1, f1: 1]]
        write(measurement, tags, dps)

        when:
        DiscreteStateQuery query = new QueryBuilder(
                key: new TimeSeriesKey(measurement, [tag1: 1]),
                timeSeries: InfluxDbTimeSeries.timeSeries('f1', intCodec))
                .discrete()
        request.addQuery(query)

        InfluxDbResult result = execute(request)

        then:
        def inHours = result.get(query).toUnitsAtStart(standardHours(1), timeZero)

        inHours.size() == 1
        inHours[0] == null
    }

    def "end of interval is excluded"() {
        given:
        def measurement = 'test'
        def tags = [tag1: 1]
        def dps = [[t: -lowerBoundInHours + 1, f1: 1], [t: 24, f1: 2]]
        write(measurement, tags, dps)

        when:
        DiscreteStateQuery query = new QueryBuilder(
                key: new TimeSeriesKey(measurement, [tag1: 1]),
                timeSeries: InfluxDbTimeSeries.timeSeries('f1', intCodec))
                .discrete()
        request.addQuery(query)

        InfluxDbResult result = execute(request)

        then:
        def inHours = result.get(query).toUnitsAtStart(standardHours(1), timeZero)

        inHours.size() == 1
        inHours[0] == 1
    }

    @SuppressWarnings("GroovyPointlessBoolean")
    def "discrete and raw (LAST() value, values in interval)"() {
        given:
        def measurement = 'test'
        def tags = [tag1: 1]
        def dps = [
                [t: -lowerBoundInHours + 1, f1: 3],
                [t: 1, f1: 1],
                [t: 8, f1: 4],
                [t: 12, f1: 2],
        ]
        write(measurement, tags, dps)

        when:
        DiscreteStateQuery discreteQuery = new QueryBuilder(
                key: new TimeSeriesKey(measurement, [tag1: 1]),
                timeSeries: InfluxDbTimeSeries.timeSeries('f1', intCodec))
                .discrete()
        request.addQuery(discreteQuery)

        RawQuery rawQuery = new QueryBuilder(
                key: new TimeSeriesKey(measurement, [tag1: 1]),
                timeSeries: InfluxDbTimeSeries.timeSeries('f1', intCodec))
                .raw()
        request.addQuery(rawQuery)

        InfluxDbResult result = execute(request)

        then:
        def descreteQueryResult = result.get(discreteQuery).toUnitsAtStart(standardHours(1), timeZero)
        descreteQueryResult[1] == 1
        descreteQueryResult[8] == 4
        descreteQueryResult[12] == 2

        and:
        def rawQueryResult = result.get(rawQuery).toUnits(standardHours(1), timeZero)
        rawQueryResult[1] == 1
        rawQueryResult[8] == 4
        rawQueryResult[12] == 2
    }

    def "grouped, discrete state query returns correct result"() {
        given:
        def measurement = 'home'
        write(measurement, [home: 1, zone: 1], [[t: -5, f1: 0], [t: 6, f1: 1], [t: 11, f1: 0]])
        write(measurement, [home: 1, zone: 2], [[t: -4, f1: 1], [t: 3, f1: 0], [t: 12, f1: 1]])
        write(measurement, [home: 1, zone: 3], [[t: -4, f1: 1]])
        write(measurement, [home: 1, zone: 4], [[t: 8, f1: 0], [t: 14, f1: 1]])

        when:
        def discreteQuery = QueryBuilder
                .forTimeSeries(measurement, [home: '1'])
                .forInfluxDbTimeSeries(InfluxDbTimeSeries.timeSeries('f1', intCodec))
                .discrete()

        def groupedQuery = new GroupedQuery<Integer>(discreteQuery, 'zone', ['1', '2', '3', '4', '5'])

        request.addQuery(groupedQuery)

        def result = execute(request).get(groupedQuery)

        then:
        def zone1Result = result['1'].toUnits(standardHours(1), timeZero)
        zone1Result.size() == 3
        zone1Result[0] == 0
        zone1Result[6] == 1
        zone1Result[11] == 0

        and:
        def zone2Result = result['2'].toUnits(standardHours(1), timeZero)
        zone2Result.size() == 3
        zone2Result[0] == 1
        zone2Result[3] == 0
        zone2Result[12] == 1

        and:
        def zone3Result = result['3'].toUnits(standardHours(1), timeZero)
        zone3Result.size() == 1
        zone3Result[0] == 1

        and:
        def zone4Result = result['4'].toUnits(standardHours(1), timeZero)
        zone4Result.size() == 3
        zone4Result[0] == null
        zone4Result[8] == 0
        zone4Result[14] == 1

        and:
        def zone5Result = result['5'].toUnits(standardHours(1), timeZero)
        zone5Result.size() == 1
        zone5Result[0] == null

        and:
        result['6'] == null
    }

    def "grouped, raw query returns correct result"() {
        given:
        def measurement = 'home'
        write(measurement, [home: 1, zone: 1], [[t: -5, f1: 0], [t: 6, f1: 1], [t: 11, f1: 0]])
        write(measurement, [home: 1, zone: 2], [[t: -4, f1: 1], [t: 3, f1: 0], [t: 12, f1: 1]])
        write(measurement, [home: 1, zone: 3], [[t: -4, f1: 1]])
        write(measurement, [home: 1, zone: 4], [[t: 8, f1: 0], [t: 14, f1: 1]])

        when:
        def raw = QueryBuilder
                .forTimeSeries(measurement, [home: '1'])
                .forInfluxDbTimeSeries(InfluxDbTimeSeries.timeSeries('f1', intCodec))
                .raw()

        def groupedQuery = new GroupedQuery<Integer>(raw, 'zone', ['1', '2', '3', '4', '5'])

        request.addQuery(groupedQuery)

        def result = execute(request).get(groupedQuery)

        then:
        def zone1Result = result['1'].toUnits(standardHours(1), timeZero)
        zone1Result.size() == 2
        zone1Result[6] == 1
        zone1Result[11] == 0

        and:
        def zone2Result = result['2'].toUnits(standardHours(1), timeZero)
        zone2Result.size() == 2
        zone2Result[3] == 0
        zone2Result[12] == 1

        and:
        def zone3Result = result['3'].toUnits(standardHours(1), timeZero)
        zone3Result.size() == 0

        and:
        def zone4Result = result['4'].toUnits(standardHours(1), timeZero)
        zone4Result.size() == 2
        zone4Result[8] == 0
        zone4Result[14] == 1

        and:
        def zone5Result = result['5'].toUnits(standardHours(1), timeZero)
        zone5Result.size() == 0

        and:
        result['6'] == null
    }

    class SimpleDomainClass implements Comparable<SimpleDomainClass> {
        int value

        @Override
        int compareTo(SimpleDomainClass o) {
            return value - o.value
        }
    }

}
