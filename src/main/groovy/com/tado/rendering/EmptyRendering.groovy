package com.tado.rendering

import grails.artefact.Controller
import org.springframework.http.HttpStatus

trait EmptyRendering extends Controller {

   void renderEmpty(HttpStatus statusCode) {
      response.status = statusCode.value()

      // A Content-Length SHOULD be set, according to https://tools.ietf.org/html/rfc7230#section-3.3.2. Since we bypass
      // the Grails rendering stuff here, we need to set it manually. Header MUST NOT be set for a 1xx or 204 response.
      if (!statusCode.is1xxInformational() && statusCode != HttpStatus.NO_CONTENT) {
         response.setContentLength(0)
      }

      response.flushBuffer()
   }

}
