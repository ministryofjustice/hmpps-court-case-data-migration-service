package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source

import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class CaseMarker(
  val id: Int,
  val typeDescription: String?,
  val created: String?,
  val createdBy: String?,
  val lastUpdated: String?,
  val lastUpdatedBy: String?,
  val deleted: Boolean?,
  val version: Int?,
)
