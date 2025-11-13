package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp
import java.util.UUID

data class DefendantOffence(
  val id: UUID?,
  val offenceID: UUID?,
  val legacyOffenceID: Long?,
  val defendantID: UUID?,
  val legacyDefendantID: Long?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
