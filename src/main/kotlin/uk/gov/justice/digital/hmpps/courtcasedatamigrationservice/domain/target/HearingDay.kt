package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp

data class HearingDay(
  val id: Int,
  val courtCentreID: Int?,
  val hearingId: Int?,
  val sittingDay: String?,
  val hearingDayTime: String?,
  val listedDurationMinutes: String?,
  val isCancelled: Boolean?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
