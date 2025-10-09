package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp

data class HearingCaseNote(
  val id: Int,
  val note: String?,
  val author: String?,
  val isDraft: Boolean?,
  val isLegacy: Boolean?,
  val createdByUUID: String?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
