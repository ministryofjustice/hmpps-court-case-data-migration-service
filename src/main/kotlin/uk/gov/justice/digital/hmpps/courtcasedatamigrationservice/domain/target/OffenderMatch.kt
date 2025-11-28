package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.math.BigDecimal
import java.sql.Timestamp
import java.util.UUID

data class OffenderMatch(
  val id: UUID?,
  val legacyID: Long?,
  val offenderID: UUID?,
  val legacyOffenderID: Long?,
  val offenderMatchGroupID: UUID?,
  val legacyOffenderMatchGroupID: Long?,
  val matchType: String?,
  val aliases: String?,
  val isRejected: Boolean?,
  val matchProbability: BigDecimal?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
