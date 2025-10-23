package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

data class CaseURN(
  val caseURN: String?,
  val createdAt: String?,
  val createdBy: String?,
  val updatedAt: String?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
