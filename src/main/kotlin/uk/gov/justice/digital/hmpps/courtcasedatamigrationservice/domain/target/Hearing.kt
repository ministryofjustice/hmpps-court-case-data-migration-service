package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp
import java.util.UUID

data class Hearing(
  val id: UUID,
  val legacyID: Long?,
  val type: String?,
  val eventType: String?,
  val listNumber: String?,
  val firstCreated: Timestamp?,
  val hearingOutcome: String?,
  val hearingCaseNote: String?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
