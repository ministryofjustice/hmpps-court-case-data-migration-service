package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

data class JudicialResult(
  val id: Int,
  val isConvictedResult: Boolean?,
  val label: String?,
  val resultTypeId: String?,
  val resultText: String?,
  val isJudicialResultDeleted: Boolean?,
  val createdAt: String?,
  val createdBy: String?,
  val updatedAt: String?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
