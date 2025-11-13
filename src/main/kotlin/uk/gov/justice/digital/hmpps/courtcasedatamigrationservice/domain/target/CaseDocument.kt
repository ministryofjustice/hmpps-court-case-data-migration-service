package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

data class CaseDocument(
  val id: Int,
  val documentID: String?,
  val documentName: String?,
  val createdAt: String?,
  val createdBy: String?,
  val updatedAt: String?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
