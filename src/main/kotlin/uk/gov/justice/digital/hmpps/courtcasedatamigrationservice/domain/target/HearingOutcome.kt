package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.util.UUID

data class HearingOutcome(
  val id: UUID,
  val legacyID: Long?,
  val defendantID: String?,
  val type: String?,
  val outcomeDate: String?,
  val state: String?,
  val assignedTo: String?,
  val assignedToUUID: String?,
  val resultedDate: String?,
  val isLegacy: Boolean?,
  val createdAt: String?,
  val createdBy: String?,
  val updatedAt: String?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
