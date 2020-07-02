package com.tado.hvaccontrol


import com.tado.zone.Zone
import groovy.transform.EqualsAndHashCode
import org.joda.time.Duration
import org.joda.time.Instant

@EqualsAndHashCode(includes = ['type', 'expiryDate'])
class TerminationCondition {

   enum Type {
      MANUAL, TADO_MODE, TIMER, NEXT_TIME_BLOCK
   }

   Type type

   Instant expiryDate
   Duration originalDuration // Overlay needs to be resettable so original duration needs to be stored

   static constraints = {
      type nullable: false
      expiryDate nullable: true
      originalDuration nullable: true
   }

   static mapping = {
      version false
   }

   static belongsTo = Overlay

   static TerminationCondition forTadoModeType() {
      return new TerminationCondition(
         type: TerminationCondition.Type.TADO_MODE,
      )
   }

   static TerminationCondition manualOnly() {
      return new TerminationCondition(type: TerminationCondition.Type.MANUAL)
   }

   static TerminationCondition timer(Duration duration) {
      return new TerminationCondition(
         type: TerminationCondition.Type.TIMER,
         originalDuration: duration,
         expiryDate: Instant.now().plus(duration)
      )
   }

   static TerminationCondition nextTimeBlock(Zone zone) {
      def nextBlockStartTime = zone.blockSchedule.findNextBlockStartTimeIgnoringMidnight(zone.getCurrentTime())
      if (nextBlockStartTime.present) {
         return new TerminationCondition(
            type: TerminationCondition.Type.NEXT_TIME_BLOCK,
            expiryDate: nextBlockStartTime.get().toInstant()
         )
      } else {
         throw new IllegalArgumentException("unsupported")
      }
   }

    TerminationCondition copy() {
      return new TerminationCondition(
         type: type,
         expiryDate: expiryDate,
         originalDuration: originalDuration
      )
   }

   @Override
   public String toString() {
      switch (type) {
         case Type.MANUAL:
            return "MANUAL"
         case Type.TADO_MODE:
            return "TADO_MODE"
         case Type.TIMER:
            return "TIMER ($originalDuration)"
         case Type.NEXT_TIME_BLOCK:
            return "NEXT_TIME_BLOCK"
         default:
            throw new UnsupportedOperationException("cannot turn overlay termination type $type into String")
      }
   }
}
