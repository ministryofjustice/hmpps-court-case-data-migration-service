package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp

data class OffenderMatchGroup(
  val id: Int,
  val prosecutionCaseId: Int?,
  val defendantId: Int?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
