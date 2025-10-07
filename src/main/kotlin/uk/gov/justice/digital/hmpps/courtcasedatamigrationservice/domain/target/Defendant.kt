package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp

data class Defendant(
  val id: Int,
  val isManualUpdate: Boolean?,
  val crn: String?,
  val croNumber: String?,
  val tsvName: String?,
  val pncId: String?,
  val cprUuid: String?,
  val isOffenderConfirmed: Boolean?,
  val person: String?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
