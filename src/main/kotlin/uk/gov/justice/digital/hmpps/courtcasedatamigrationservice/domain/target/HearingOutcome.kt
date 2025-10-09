package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp

data class HearingOutcome(
  val id: Int,
  val type: String?,
  val outcomeDate: Timestamp?,
  val state: String?,
  val assignedTo: String?,
  val assignedToUUID: String?,
  val resultedDate: Timestamp?,
  val isLegacy: Boolean?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
