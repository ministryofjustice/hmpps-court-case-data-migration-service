package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp

data class HearingOutcome(
  val id: Int,
  val defendantId: String?,
  val type: String?,
  val outcomeDate: Timestamp?,
  val state: String?,
  val assignedTo: String?,
  val assignedToUUID: String?,
  val resultedDate: Timestamp?,
  val isLegacy: Boolean?,
  val createdAt: String?,
  val createdBy: String?,
  val updatedAt: String?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
