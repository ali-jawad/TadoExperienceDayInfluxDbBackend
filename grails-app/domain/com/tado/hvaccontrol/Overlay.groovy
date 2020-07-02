package com.tado.hvaccontrol

import com.tado.zone.Zone

class Overlay<SettingType extends UserSetting> {

   static belongsTo = Zone

   SettingType setting  // this results in a proper GORM association mapping to UserSetting since SettingType has UserSetting as a bound

   TerminationCondition terminationCondition

   static constraints = {
      setting nullable: false, validator: { it.validate() } // for unknown reasons, the setting object is not validated when validating the overlay
      terminationCondition nullable: false
   }

   static mapping = {
      setting cascade: "all", fetch: 'join' // embedding not possible, therefore loading eagerly
      version false
   }

   @Override
   public String toString() {
      def typeString = "$terminationCondition.type"

      if (terminationCondition.type == TerminationCondition.Type.TIMER)
         typeString += "(${terminationCondition})"

      return "$typeString, Setting: $setting"
   }

}
