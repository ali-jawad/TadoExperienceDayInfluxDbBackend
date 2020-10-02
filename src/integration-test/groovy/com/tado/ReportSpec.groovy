package com.tado

import com.tado.testutils.TestHomes
import com.tado.testutils.TestTimeSeries
import geb.spock.GebSpec
import grails.testing.mixin.integration.Integration
import groovy.json.JsonSlurper
import org.junit.Rule

@Integration
class ReportSpec extends GebSpec {

    @Rule
    TestHomes homes
    @Rule
    TestTimeSeries timeSeries

    void "simple example"() {
        when: "the report is retrieved"
        go "/report"

        then: "the answer is empty"
        new JsonSlurper().parseText(driver.pageSource) == [:]
    }
}
