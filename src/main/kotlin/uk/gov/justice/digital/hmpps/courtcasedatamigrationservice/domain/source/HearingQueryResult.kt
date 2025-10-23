package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source

import java.sql.Timestamp

data class HearingQueryResult(
  val id: Int,
  val hearingType: String?,
  val hearingEventType: String?,
  val listNo: String?,
  val firstCreated: Timestamp?,
  val hearingOutcomes: String?,
  val hearingNotes: String?,
  val created: Timestamp?,
  val createdBy: String?,
  val lastUpdated: Timestamp?,
  val lastUpdatedBy: String?,
  val deleted: Boolean?,
  val version: Int?,
)
