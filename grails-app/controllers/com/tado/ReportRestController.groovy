package com.tado

import com.tado.rendering.EmptyRendering
import com.tado.rendering.JsonRendering
import org.springframework.http.HttpStatus

class ReportRestController implements JsonRendering, EmptyRendering {

    def showReport() {
        renderJson(HttpStatus.OK, [:])
    }
}
