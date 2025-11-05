package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp

data class OffenderMatch(
  val id: Int,
  val offenderId: Long?,
  val offenderMatchGroupId: Long?,
  val matchType: String?,
  val aliases: String?,
  val isRejected: Boolean,
  val matchProbability: Double?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
