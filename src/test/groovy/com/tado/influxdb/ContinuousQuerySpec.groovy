package com.tado.influxdb

import com.tado.influxdb.codecs.IdentityInfluxDbCodec
import com.tado.influxdb.queries.ContinuousQuery
import com.tado.influxdb.queries.QueryBuilder
import org.influxdb.InfluxDB
import org.influxdb.InfluxDBFactory
import org.influxdb.dto.BatchPoints
import org.influxdb.dto.Point
import org.joda.time.Instant
import org.joda.time.Period
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static org.joda.time.Duration.standardHours
import static org.joda.time.Duration.standardMinutes

class ContinuousQuerySpec extends Specification {

    @Shared
    InfluxDB influxDb
    @Shared
    String database

    @Shared
    String measurement = 'test'
    @Shared
    def tags = [tag1: 1]

    @Shared
    double a = 1, b = 2, c = 3, d = 4, e = 5, f = 6, g = 7, h = 8

    @Shared
    ContinuousQuery query = buildContinuousQuery('f1')

    @Shared
    Instant timeZero = new Instant(1451606400000) // 01.01.2016
    @Shared
    Instant lowerBound = timeZero - standardHours(24 * 7)

    private ContinuousQuery buildContinuousQuery(String field) {
        new QueryBuilder(
                key: new TimeSeriesKey(measurement, [tag1: 1]),
                timeSeries: InfluxDbTimeSeries.timeSeries(field, new IdentityInfluxDbCodec<Double>()))
                .continuous(Period.minutes(30), ContinuousQuery.Aggregation.MEAN)
    }

    def setupSpec() {
        influxDb = InfluxDBFactory.connect('http://localhost:8087', 'u', 'p')
        influxDb.setLogLevel(InfluxDB.LogLevel.FULL)

        database = "cont_query_spec_${UUID.randomUUID().toString().replaceAll('-', '_')}"
        influxDb.createDatabase(database)

        BatchPoints.Builder builder = BatchPoints.database(database)
        builder.point(p(0 * 60 + 35, a))
        builder.point(p(0 * 60 + 50, b))
        builder.point(p(1 * 60 + 5, c))
        builder.point(p(1 * 60 + 10, d))
        builder.point(p(1 * 60 + 55, e))
        builder.point(p(2 * 60 + 30, f))
        builder.point(p(2 * 60 + 40, g))
        builder.point(p(2 * 60 + 55, h))
        influxDb.write(builder.build())
    }

    private Point p(int minutes, Double f1, String field = 'f1') {
        Point.measurement(measurement)
                .tag(tags.collectEntries { [it.key, it.value.toString()] } as Map<String, String>)
                .time(timeZero.millis + standardMinutes(1).millis * minutes, TimeUnit.MILLISECONDS)
                .addField(field, f1)
                .build()
    }

    def cleanupSpec() {
        influxDb.deleteDatabase(database)
    }

    private Map<Double, Double> query(int startOffsetMinutes, int endOffsetMinutes) {
        return query(startOffsetMinutes, endOffsetMinutes, query).first()
    }

    private List<Map<Double, Double>> query(int startOffsetMinutes, int endOffsetMinutes, ContinuousQuery... queries) {
        def request = new InfluxDbRequest(
                timeZero + standardMinutes(startOffsetMinutes),
                timeZero + standardMinutes(endOffsetMinutes),
                lowerBound)
        queries.each { request.addQuery(it) }

        def results = new QueryInfluxDbCommand(influxDb, database, request.statements).execute()
        InfluxDbResult result = request.createResult(results)

        return queries.collect {
            result.get(it).toUnits(standardMinutes(1), timeZero)
        }
    }

    def "start and end are aligned with the buckets"() {
        when:
        Map<Double, Double> points = query(60, 3 * 60)

        then:
        points.size() == 5
        points[1 * 60 + 0] == (b + c + d) / 3d
        points[1 * 60 + 30] == (b + c + d) / 3d
        points[2 * 60 + 0] == e
        points[2 * 60 + 30] == (f + g) / 2d
        points[3 * 60 + 0] == h
    }

    def "no values in first bucket"() {
        when:
        Map<Double, Double> points = query(1 * 60 + 30, 3 * 60)

        then:
        points.size() == 4
        points[1 * 60 + 30] == d
        points[2 * 60 + 0] == e
        points[2 * 60 + 30] == (f + g) / 2d
        points[3 * 60 + 0] == h
    }

    def "no values in query interval, but LAST value before"() {
        when:
        Map<Double, Double> points = query(3 * 60, 4 * 60)

        then:
        points.size() == 3
        points[3 * 60 + 0] == h
        points[3 * 60 + 30] == h
        points[4 * 60] == h
    }

    def "no values in query interval at all, no LAST value before"() {
        when:
        Map<Double, Double> points = query(-1 * 60, 0 * 60)

        then:
        points.size() == 3
        points[-1 * 60 + 0] == null
        points[-1 * 60 + 30] == null
        points[0 * 60 + 0] == null
    }

    def "no values in query interval, but right after, no LAST value before"() {
        when:
        Map<Double, Double> points = query(-1 * 60, 0 * 60 + 30)

        then:
        points.size() == 4
        points[-1 * 60 + 0] == null
        points[-1 * 60 + 30] == null
        points[0 * 60 + 0] == null
        points[0 * 60 + 30] == 1.0
    }

    def "start is in the first half of the first bucket"() {
        when:
        Map<Double, Double> points = query(55, 3 * 60)

        then:
        points.size() == 6
        points[55] == b
        points[60] == (b + c + d) / 3d
        // no need to test the middle points again
    }

    def "start is in the second half of a bucket before the first bucket"() {
        when:
        Map<Double, Double> points = query(35, 3 * 60)

        then:
        points.size() == 6
        points[35] == a
        points[60] == (b + c + d) / 3d
        // no need to test the middle points again
    }

    def "end is in the first half of the last bucket"() {
        when:
        Map<Double, Double> points = query(60, 2 * 60 + 50)

        then:
        points.size() == 5
        // no need to test the middle points again
        points[2 * 60 + 50] == g
    }

    def "end is in the second half of the last bucket"() {
        when:
        Map<Double, Double> points = query(60, 3 * 60 + 10)

        then:
        points.size() == 6
        // no need to test the middle points again
        points[3 * 60 + 0] == h
        points[3 * 60 + 10] == h
    }

    def "two queries for different fields of the same series"() {
        setup:
        BatchPoints.Builder builder = BatchPoints.database(database)
        builder.point(p(0 * 60 + 35, a + 5d, 'f2'))
        builder.point(p(0 * 60 + 50, b + 5d, 'f2'))
        builder.point(p(1 * 60 + 5, c + 5d, 'f2'))
        builder.point(p(1 * 60 + 10, d + 5d, 'f2'))
        builder.point(p(1 * 60 + 55, e + 5d, 'f2'))
        builder.point(p(2 * 60 + 30, f + 5d, 'f2'))
        builder.point(p(2 * 60 + 40, g + 5d, 'f2'))
        builder.point(p(2 * 60 + 55, h + 5d, 'f2'))
        influxDb.write(builder.build())

        ContinuousQuery secondQuery = buildContinuousQuery('f2')

        when:
        List<Map<Double, Double>> points = query(60, 3 * 60 + 10, query, secondQuery)

        then:
        def firstQueryPoints = points[0]
        firstQueryPoints.size() == 6
        firstQueryPoints[1 * 60 + 0] == (b + c + d) / 3d
        firstQueryPoints[1 * 60 + 30] == (b + c + d) / 3d
        firstQueryPoints[2 * 60 + 0] == e
        firstQueryPoints[2 * 60 + 30] == (f + g) / 2d
        firstQueryPoints[3 * 60] == h
        firstQueryPoints[3 * 60 + 10] == h

        and:
        def secondQueryPoints = points[1]
        secondQueryPoints.size() == 6
        secondQueryPoints[1 * 60 + 0] == ((b + 5d) + (c + 5d) + (d + 5d)) / 3d
        secondQueryPoints[1 * 60 + 30] == ((b + 5d) + (c + 5d) + (d + 5d)) / 3d
        secondQueryPoints[2 * 60 + 0] == e + 5d
        secondQueryPoints[2 * 60 + 30] == ((f + 5d) + (g + 5d)) / 2d
        secondQueryPoints[3 * 60] == h + 5d
        secondQueryPoints[3 * 60 + 10] == h + 5d
    }

    def "two queries with no data points for one of the series"() {
        setup:
        ContinuousQuery secondQuery = buildContinuousQuery('f3')

        when:
        List<Map<Double, Double>> points = query(60, 3 * 60 + 10, query, secondQuery)

        then:
        def firstQueryPoints = points[0]
        firstQueryPoints.size() == 6
        firstQueryPoints[1 * 60 + 0] == (b + c + d) / 3d
        firstQueryPoints[1 * 60 + 30] == (b + c + d) / 3d
        firstQueryPoints[2 * 60 + 0] == e
        firstQueryPoints[2 * 60 + 30] == (f + g) / 2d
        firstQueryPoints[3 * 60] == h
        firstQueryPoints[3 * 60 + 10] == h

        and:
        def secondQueryPoints = points[1]
        secondQueryPoints.size() == 6
        secondQueryPoints[1 * 60 + 0] == null
        secondQueryPoints[1 * 60 + 30] == null
        secondQueryPoints[2 * 60 + 0] == null
        secondQueryPoints[2 * 60 + 30] == null
        secondQueryPoints[3 * 60] == null
        secondQueryPoints[3 * 60 + 10] == null
    }

}
