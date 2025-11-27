package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

data class HearingOutcome(
  val id: Int,
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
