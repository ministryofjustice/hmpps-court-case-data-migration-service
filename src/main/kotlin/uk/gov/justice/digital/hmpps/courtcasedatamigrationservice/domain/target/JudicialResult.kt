package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp

data class JudicialResult(
  val id: Int,
  val isConvictedResult: Boolean?,
  val label: String?,
  val resultTypeId: Int?,
  val isJudicialResultDeleted: Boolean?,
  val createdAt: String?,
  val createdBy: String?,
  val updatedAt: String?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
