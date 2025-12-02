package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.util.UUID

data class HearingCaseNote(
  val id: UUID,
  val legacyID: Long?,
  val defendantID: String?,
  val note: String?,
  val author: String?,
  val isDraft: Boolean?,
  val isLegacy: Boolean?,
  val createdByUUID: String?,
  val createdAt: String?,
  val createdBy: String?,
  val updatedAt: String?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
