package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp

data class Defendant(
  val id: Int,
  val masterDefendantId: String?,
  val numberOfPreviousConvictionsCited: String?,
  val manualUpdate: String?,
  val mitigation: String?,
  val crn: String?,
  val croNumber: String?,
  val isYouth: Boolean?,
  val tsvName: String?,
  val pncId: Int,
  val isProceedingsConcluded: Boolean?,
  val cprUuid: String?,
  val offenderConfirmed: String?,
  val person: String?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
