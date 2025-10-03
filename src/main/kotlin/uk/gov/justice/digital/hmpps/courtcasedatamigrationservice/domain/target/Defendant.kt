package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp

data class Defendant(
  val id: Int,
  val masterDefendantId: Int?,
  val numberOfPreviousConvictionsCited: String?,
  val isManualUpdate: Boolean?,
  val mitigation: String?,
  val crn: String?,
  val croNumber: String?,
  val isYouth: Boolean?,
  val tsvName: String?,
  val pncId: Int?,
  val isProceedingsConcluded: Boolean?,
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
