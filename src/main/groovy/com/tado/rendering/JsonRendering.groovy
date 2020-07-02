package com.tado.rendering

import grails.artefact.Controller
import grails.converters.JSON
import grails.web.mime.MimeType
import org.springframework.http.HttpStatus

trait JsonRendering extends Controller {

   void renderJson(HttpStatus status, Map<String, ?> bodyMap) {
      render(status: status.value(), contentType: MimeType.JSON.name, text: bodyMap as JSON)
   }

   void renderJson(HttpStatus status, List<?> bodyList) {
      render(status: status.value(), contentType: MimeType.JSON.name, text: bodyList as JSON)
   }

   void renderJsonApiError(HttpStatus status, String code, String title, Map additionalProperties = [:]) {
      def singleError = [
         code : code,
         title: title
      ] + additionalProperties
      def bodyMap = [errors: [singleError]]

      renderJson(status, bodyMap)
   }

   void renderNotFound(String type, Object id) {
      renderJsonApiError(HttpStatus.NOT_FOUND, 'notFound', "$type $id not found")
   }

   void renderForbidden(String resource, String reason = null) {
      def title = "current user is not allowed to access $resource"
      if (reason != null) {
         title += " ($reason)"
      }
      renderJsonApiError(HttpStatus.FORBIDDEN, 'accessDenied', title)
   }
}
