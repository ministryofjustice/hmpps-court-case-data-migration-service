package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source

import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class CourtRoom(
  val id: Int,
  val courtRoom: String?,
)
