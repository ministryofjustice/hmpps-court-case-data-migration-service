package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp
import java.util.UUID

data class Defendant(
  val id: UUID?,
  val defendantID: UUID?,
  val legacyID: Long?,
  val isManualUpdate: Boolean?,
  val crn: String?,
  val croNumber: String?,
  val tsvName: String?,
  val pncId: String?,
  val cprUUID: String?,
  val isOffenderConfirmed: Boolean?,
  val person: String?,
  val legacyOffenderID: Long?,
  val offenderID: UUID?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
