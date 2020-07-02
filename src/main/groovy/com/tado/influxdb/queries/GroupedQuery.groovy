package com.tado.influxdb.queries

import com.tado.influxdb.TimeSeriesKeyAndName
import com.tado.influxdb.queries.RawQuery

class GroupedQuery<DomainType> {

   private final Query<DomainType> groupedQuery
   private final String groupByTag
   private final Collection<String> groupByTagValues

   GroupedQuery(DiscreteStateQuery<DomainType> groupedQuery, String groupByTag, Collection<String> groupByTagValues) {
      this.groupedQuery = groupedQuery
      this.groupByTag = groupByTag
      this.groupByTagValues = groupByTagValues
   }

   GroupedQuery(RawQuery<DomainType> groupedQuery, String groupByTag, Collection<String> groupByTagValues) {
      this.groupedQuery = groupedQuery
      this.groupByTag = groupByTag
      this.groupByTagValues = groupByTagValues
   }

    Query<DomainType> getGroupedQuery() {
      return groupedQuery
   }

   String getGroupByTag() {
      return groupByTag
   }

   Collection<String> getGroupByTagValues() {
      return groupByTagValues
   }

    TimeSeriesKeyAndName getSubKeyAndName(String subKeyTagValue) {
      assert subKeyTagValue in groupByTagValues
      return groupedQuery.keyAndName.forSubTag(groupByTag, subKeyTagValue)
   }

   List<String> getFields() {
      return groupedQuery.fields
   }

}
