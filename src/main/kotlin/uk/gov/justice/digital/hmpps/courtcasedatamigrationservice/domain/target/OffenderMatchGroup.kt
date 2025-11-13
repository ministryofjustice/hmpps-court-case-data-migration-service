package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp
import java.util.UUID

data class OffenderMatchGroup(
  val id: UUID?,
  val legacyID: Long?,
  val prosecutionCaseID: UUID?,
  val legacyProsecutionCaseID: Long?,
  val defendantID: UUID?,
  val legacyDefendantID: Long?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
