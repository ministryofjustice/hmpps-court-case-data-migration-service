package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source

import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming
import java.sql.Timestamp

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class HearingOutcome(
  val id: Int,
  val defendantId: String?,
  val outcomeType: String?,
  val outcomeDate: Timestamp?,
  val state: String?,
  val assignedTo: String?,
  val assignedToUuid: String?,
  val resultedDate: Timestamp?,
  val legacy: Boolean?,
  val created: Timestamp?,
  val createdBy: String?,
  val lastUpdated: Timestamp?,
  val lastUpdatedBy: String?,
  val deleted: Boolean?,
  val version: Int?,
)
