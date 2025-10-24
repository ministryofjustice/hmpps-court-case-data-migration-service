package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import com.fasterxml.jackson.annotation.JsonProperty

data class CaseURN(
  val caseURN: String?,
  val createdAt: String?,
  val createdBy: String?,
  val updatedAt: String?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)

data class CaseURNs(
  @JsonProperty("caseURNs")
  val caseURNs: List<CaseURN>,
)
