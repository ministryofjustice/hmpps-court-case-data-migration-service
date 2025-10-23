package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source

import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming
import java.sql.Time
import java.sql.Timestamp
import java.time.LocalDate

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class HearingNote(
  val id: Int,
  val defendantId: String?,
  val note: String?,
  val author: String?,
  val draft: Boolean?,
  val legacy: Boolean?,
  val createdByUuid: String?,
  val created: Timestamp?,
  val createdBy: String?,
  val lastUpdated: Timestamp?,
  val lastUpdatedBy: String?,
  val deleted: Boolean?,
  val version: Int?,
)
