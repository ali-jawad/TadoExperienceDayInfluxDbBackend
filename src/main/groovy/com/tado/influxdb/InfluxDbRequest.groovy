package com.tado.influxdb

import com.google.common.collect.Lists
import com.tado.influxdb.queries.ContinuousQuery
import com.tado.influxdb.queries.DiscreteStateQuery
import com.tado.influxdb.queries.GroupedQuery
import com.tado.influxdb.queries.Query
import com.tado.influxdb.queries.QueryBuilder
import com.tado.influxdb.queries.RawQuery
import com.tado.timeseries.DataPoint
import com.tado.timeseries.DataPointList
import org.influxdb.dto.QueryResult
import org.joda.time.Duration
import org.joda.time.Instant
import org.joda.time.Interval
import org.joda.time.Period
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.ISODateTimeFormat

final class InfluxDbRequest {

   public final static String LAST_FIELD_NAME_PREFIX = 'last_'
   private final static String LAST_FIELD_FOR_END_SUFFIX = '_at_end'

   final Instant start
   final Instant end
   final Instant lowerBoundTime
   private List<DiscreteStateQuery> discreteStateQueries = []
   private List<ContinuousQuery> continuousQueries = []
   private List<RawQuery> rawQueries = []
   private List<GroupedQuery> groupedQueries = []

   /**
    * @param start of the result we are interested in.
    * @param end of the result we are interested in.
    * @param lowerBoundTime applicable to non-raw queries; It indicates how far back in time we should search until
    *          we try to find value corresponding to the first point-in-time in interest (i.e. start).
    */
   InfluxDbRequest(Instant start, Instant end, Instant lowerBoundTime) {
      this.start = start
      this.end = end
      this.lowerBoundTime = lowerBoundTime
   }

   InfluxDbRequest(Interval interval, Instant lowerBoundTime) {
      this(interval.start.toInstant(), interval.end.toInstant(), lowerBoundTime)
   }

   private boolean isEndAligned(Period bucketSize) {
      end.millis % bucketSize.toStandardDuration().millis == 0
   }

   public void addQuery(DiscreteStateQuery query) {
      discreteStateQueries << query
   }

   public void addQuery(ContinuousQuery query) {
      continuousQueries << query
   }

   public <T> void addQuery(RawQuery query) {
      rawQueries << query
   }

   public <T> void addQuery(GroupedQuery query) {
      groupedQueries << query
   }

   private String lastFieldName(String field) {
      "${LAST_FIELD_NAME_PREFIX}${field}"
   }

   private String lastForEndFieldName(String field) {
      "${LAST_FIELD_NAME_PREFIX}${field}${LAST_FIELD_FOR_END_SUFFIX}"
   }

   private String format(Instant instant) {
      // The highest precision for Instants is millis, so there is no point in having micro/nanosecond precision.
      // Even if the response precision can be controlled through `precision` in this class, the query timestamps
      // should always be full precision!
      return "${instant.millis}ms"
      // Alternatively, this also works:
      // return "'${ISODateTimeFormat.dateTime().print(instant)}'"
   }

   String timeRestriction(Instant from, Instant to, boolean includeEnd = false) {
      "time >= ${format(from)} AND time <${includeEnd ? '=' : ''} ${format(to)}"
   }

   String tagRestriction(Map<String, String> tags) {
      tags.collect { String tagKey, String tagValue -> "$tagKey = '$tagValue'" }.join(' AND ')
   }

   String restriction(Instant from, Instant to, Map<String, String> tags, boolean includeEnd = false) {
      if (tags.isEmpty()) {
         return timeRestriction(from, to, includeEnd)
      } else {
         return "${tagRestriction(tags)} AND ${timeRestriction(from, to, includeEnd)}"
      }
   }

   String groupRestriction(Instant from, Instant to, Map<String, String> tags, String groupByTag, Collection<String> groupByTagValues) {
      def groupRestriction = '(' + groupByTagValues.collect { "$groupByTag = '$it'" }.join(' OR ') + ')'
      if (tags.isEmpty()) {
         return "$groupRestriction AND ${timeRestriction(from, to)}"
      } else {
         return "${tagRestriction(tags)} AND $groupRestriction AND ${timeRestriction(from, to)}"
      }
   }

   String getQueryString() {
      return getStatements().join('\n')
   }

   List<String> getStatements() {
      // Note we are consciously double-quoting the field identifiers. See the comment at the bottom of
      // https://github.com/tadodotcom/TadoGrailsApp/issues/14526
      // for an explanation of the issue.

      List<? extends Query> combinableDataQueries = [] + discreteStateQueries + rawQueries // [] + makes intellij happy
      List<String> combinedDataQueries = combinableDataQueries
         .groupBy { it.key }
         .collect { TimeSeriesKey key, List<? extends Query> queries ->
            def combinedFields = queries.collect { it.fields }.flatten().collect { /"$it"/ } as Set
            String restriction = restriction(start, end, key.tags)
            String from = from(key)
            return "SELECT ${combinedFields.join(',')} FROM $from WHERE $restriction GROUP BY ${key.tags.keySet().join(',')};"
         }

      List<String> combinedLastQueries = discreteStateQueries
         .groupBy { it.key }
         .collect { TimeSeriesKey key, List<? extends Query> queries ->
            def combinedLastFields = queries.collect { it.fields }.flatten().collect {
               "LAST(\"${it}\") AS \"${lastFieldName(it)}\""
            } as Set
            String restriction = restriction(lowerBoundTime, start, key.tags)
            String from = from(key)
            return "SELECT ${combinedLastFields.join(',')} FROM $from WHERE $restriction GROUP BY ${key.tags.keySet().join(',')};"
         }

      def combinableContinuousQueries = continuousQueries.groupBy { [it.key, it.bucketSize] }
      List<String> continuousDataQueries = combinableContinuousQueries.collect { List key, List<ContinuousQuery> queries ->
         TimeSeriesKey seriesKey = key[0]
         Period bucketSize = key[1]

         def fields = queries.collect { query ->
            query.fields.collect {
               "$query.aggregation(\"$it\") AS \"$it\""
            }
         }.flatten() as Set

         Duration bucketDuration = bucketSize.toStandardDuration()
         Duration halfBucketDuration = bucketDuration.dividedBy(2)
         String restriction = restriction(start - halfBucketDuration, end + halfBucketDuration, seriesKey.tags)

         // use the GROUP BY clause so that the resulting Series object has a 'tags' map that we can use to find the correct series
         String time = "time(${bucketDuration.toStandardSeconds().seconds}s,${halfBucketDuration.toStandardSeconds().seconds}s)"
         def groups = [time] + seriesKey.tags.keySet()
         return "SELECT ${fields.join(',')} FROM ${seriesKey.measurement} WHERE $restriction GROUP BY ${groups.join(',')} FILL(previous);"
      }

      List<String> continuousLastQueries = combinableContinuousQueries.collect { List key, List<ContinuousQuery> queries ->
         TimeSeriesKey seriesKey = key[0]

         def fields = queries.collect { query ->
            query.fields.collect {
               "LAST(\"${it}\") AS \"${lastFieldName(it)}\""
            }
         }.flatten() as Set

         // Include the end since there could be a value right on 'start'. This is not the case for DS queries since a
         // potential value on 'start' is found by the data query.
         String restriction = restriction(lowerBoundTime, start, seriesKey.tags, true)

         return "SELECT ${fields.join(',')} FROM ${seriesKey.measurement} WHERE $restriction GROUP BY ${seriesKey.tags.keySet().join(',')};"
      }

      List<String> continuousLastQueriesForEnd = combinableContinuousQueries.findAll {
         Period bucketSize = it.key[1]
         !isEndAligned(bucketSize)
      }.collect { List key, List<ContinuousQuery> queries ->
         TimeSeriesKey seriesKey = key[0]
         def fields = queries.collect { query ->
            query.fields.collect {
               "LAST(\"${it}\") AS \"${lastForEndFieldName(it)}\""
            }
         }.flatten() as Set

         // Include the end since there could be a value right on 'end'. This is not the case for DS queries since a
         // potential value on 'end' is found by the data query.
         String restriction = restriction(lowerBoundTime, end, seriesKey.tags, true)

         return "SELECT ${fields.join(',')} FROM ${seriesKey.measurement} WHERE $restriction GROUP BY ${seriesKey.tags.keySet().join(',')};"
      }

      List<String> combinedGroupedDataQueries = groupedQueries
         .groupBy { [it.groupedQuery.key, it.groupByTag, it.groupByTagValues] }
         .collect { List groupKey, List<GroupedQuery> queries ->
            def groupedQueryKey = groupKey[0] as TimeSeriesKey
            def groupByTag = groupKey[1] as String
            def groupByTagValues = groupKey[2] as Collection<String>

            def combinedFields = queries.collect { it.fields }.flatten().collect { /"$it"/ } as Set
            String restriction = groupRestriction(start, end, groupedQueryKey.tags, groupByTag, groupByTagValues)
            String from = from(groupedQueryKey)
            return "SELECT ${combinedFields.join(',')} FROM $from WHERE $restriction GROUP BY ${(groupedQueryKey.tags.keySet() + [groupByTag]).join(',')};"
         }

      List<String> combinedGroupedLastQueries = groupedQueries
         .groupBy { [it.groupedQuery.key, it.groupByTag, it.groupByTagValues] }
         .collect { List groupKey, List<GroupedQuery> queries ->
            def groupedQueryKey = groupKey[0] as TimeSeriesKey
            def groupByTag = groupKey[1] as String
            def groupByTagValues = groupKey[2] as Collection<String>

            def combinedLastFields = queries.collect { it.fields }.flatten().collect {
               "LAST(\"${it}\") AS \"${lastFieldName(it)}\""
            } as Set

            String restriction = groupRestriction(lowerBoundTime, start, groupedQueryKey.tags, groupByTag, groupByTagValues)
            String from = from(groupedQueryKey)
            return "SELECT ${combinedLastFields.join(',')} FROM $from WHERE $restriction GROUP BY ${(groupedQueryKey.tags.keySet() + [groupByTag]).join(',')};"
         }

      return combinedDataQueries + combinedLastQueries + continuousDataQueries + continuousLastQueries + continuousLastQueriesForEnd + combinedGroupedDataQueries + combinedGroupedLastQueries
   }

   private String from(TimeSeriesKey key) {
      if (key.retentionPolicy.present) {
         return "${key.retentionPolicy.get()}.$key.measurement"
      } else {
         return "$key.measurement"
      }
   }

   private QueryResult.Series findSeriesNullable(List<QueryResult.Result> results, TimeSeriesKey key, List<String> fields) {
      // findAll: if there is no values for a query, the entire series is empty: no name, no tags, no columns, ... :-(
      return results*.series.flatten().findAll().find {
         it.name == key.measurement &&
            it.tags == key.tags &&
            it.columns.containsAll(fields)
      } as QueryResult.Series
   }

   final static DateTimeFormatter parser = ISODateTimeFormat.dateTimeParser()

   static Instant parse(Object influxDbTimestamp) {
      return new Instant(influxDbTimestamp as long)
   }

   InfluxDbResult createResult(List<QueryResult.Result> results) {
      if (results.any { it.hasError() }) {
         int errorIdx = results.findIndexOf { it.hasError() }
         throw new RuntimeException("one query has error: '${results[errorIdx].error}' for query ${getStatements()[errorIdx]}")
      }

      def discreteStateQueriesToProcess = Lists.newArrayList(discreteStateQueries)
      def rawQueriesToProcess = Lists.newArrayList(rawQueries)

      // @formatter:off
      def groupedDiscreteStateQueries = groupedQueries
         .findAll { it.groupedQuery instanceof DiscreteStateQuery }
         .collect { def query ->
            query.groupByTagValues.collect() {
               def keyAndName = query.getSubKeyAndName(it)
               QueryBuilder
                  .forTimeSeries(keyAndName.key)
                  .forInfluxDbTimeSeries(query.groupedQuery.timeSeries)
                  .discrete()
            }
         }
         .flatten() as List<DiscreteStateQuery<?>>;
      // @formatter:on
      discreteStateQueriesToProcess.addAll(groupedDiscreteStateQueries)

      // @formatter:off
      def groupedRawQueries = groupedQueries
         .findAll { it.groupedQuery instanceof RawQuery }
         .collect { def query ->
            query.groupByTagValues.collect {
               def keyAndName = query.getSubKeyAndName(it)
               QueryBuilder
                  .forTimeSeries(keyAndName.key)
                  .forInfluxDbTimeSeries(query.groupedQuery.timeSeries)
                  .raw()
            }
         }
         .flatten() as List<RawQuery<?>>;
      // @formatter:on
      rawQueriesToProcess.addAll(groupedRawQueries)

      def discreteStateResults = discreteStateQueriesToProcess.collectEntries { DiscreteStateQuery query ->
         def fields = query.fields
         def series = findSeriesNullable(results, query.key, fields)
         def lastValueSeries = findSeriesNullable(results, query.key, fields.collect { lastFieldName(it) })
          InfluxDbFieldMap firstValue = InfluxDbDataPointUtils.parseLastValue(lastValueSeries, fields, LAST_FIELD_NAME_PREFIX)
         def dpl = InfluxDbDataPointUtils
            .createListFromSeries(series)
            .transformValues { it.project(fields) }
            .findAll { !it.allValuesNull() }
            .startWith(start, firstValue)

         return [query.keyAndName, dpl]
      }

      def continuousResults = continuousQueries.collectEntries { ContinuousQuery query ->
         assert query.fields.size() == 1
         def field = query.fields.first()

         def series = findSeriesNullable(results, query.key, query.fields)
         def lastValueSeries = findSeriesNullable(results, query.key, query.fields.collect {
            lastFieldName(it)
         })
         def lastValueAtEndSeries = findSeriesNullable(results, query.key, query.fields.collect {
            lastForEndFieldName(it)
         })
         def lastValue = InfluxDbDataPointUtils.parseLastValue(lastValueSeries, [field], LAST_FIELD_NAME_PREFIX).unwrapSingleValue()
         // passing field + LAST_FIELD_FOR_END_SUFFIX is a bit hacky
         def valueAtEnd = isEndAligned(query.bucketSize) ? null : InfluxDbDataPointUtils.parseLastValue(lastValueAtEndSeries, [field + LAST_FIELD_FOR_END_SUFFIX], LAST_FIELD_NAME_PREFIX).unwrapSingleValue()

         def dpl = InfluxDbDataPointUtils
            .createListFromSeries(series)
            .transformValues { InfluxDbFieldMap map -> map.project([field]) }
            .transformValues { InfluxDbFieldMap map -> map.unwrapSingleValue() }
            .shift(query.bucketSize.toStandardDuration().dividedBy(2))  // shift time to middle of bucket
            .or(DataPointList.grid(start, end, query.bucketSize.toStandardDuration(), null))

         def newPoints = new ArrayList<DataPoint<?>>(dpl.toList())

         // fill first buckets if empty
         int i = 0
         while (i < newPoints.size() && newPoints[i].value == null) {
            newPoints[i] = newPoints[i].transform { lastValue }
            i++
         }

         // replace start point if start of query is not aligned to buckets
         if (newPoints.first().timestamp != start) {
            newPoints[0] = com.tado.timeseries.DataPoint.dp(start, lastValue)
         }

         // replace end point if end of query is not aligned to buckets
         if (newPoints.last().timestamp != end) {
            newPoints[-1] = com.tado.timeseries.DataPoint.dp(end, valueAtEnd)
         }

         dpl = DataPointList
            .fromSortedList(newPoints)
            .transformValues { return InfluxDbFieldMap.createWithSingleField(field, it) }

         return [query.keyAndName, dpl]
      }

      def rawResults = rawQueriesToProcess.collectEntries { RawQuery query ->
         List<String> fields = query.fields

         def series = findSeriesNullable(results, query.key, query.fields)

         def decodedRawPoints = InfluxDbDataPointUtils
            .createListFromSeries(series)
            .transformValues { InfluxDbFieldMap map -> map.project(fields) }
            .findAll { InfluxDbFieldMap map -> !map.allValuesNull() }

         return [query.keyAndName, decodedRawPoints]
      }

      return new InfluxDbResult(end, discreteStateResults, rawResults, continuousResults)
   }

}
