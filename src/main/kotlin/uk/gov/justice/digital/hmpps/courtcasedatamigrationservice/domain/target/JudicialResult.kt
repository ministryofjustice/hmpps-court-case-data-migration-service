package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.util.UUID

data class JudicialResult(
  val id: UUID,
  val legacyID: Long?,
  val isConvictedResult: Boolean?,
  val label: String?,
  val resultTypeID: String?,
  val resultText: String?,
  val isJudicialResultDeleted: Boolean?,
  val createdAt: String?,
  val createdBy: String?,
  val updatedAt: String?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
