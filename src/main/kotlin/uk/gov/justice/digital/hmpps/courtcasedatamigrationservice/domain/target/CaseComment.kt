package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp
import java.util.UUID

data class CaseComment(
  val id: UUID,
  val legacyID: Long?,
  val defendantID: UUID?,
  val legacyDefendantID: UUID?,
  val caseID: UUID?,
  val legacyCaseID: String?,
  val author: String?,
  val comment: String?,
  val isDraft: Boolean?,
  val isLegacy: Boolean?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
