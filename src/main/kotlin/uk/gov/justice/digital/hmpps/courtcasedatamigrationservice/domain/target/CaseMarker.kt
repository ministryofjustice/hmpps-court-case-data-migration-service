package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

data class CaseMarker(
  val id: Int,
  val typeId: String?,
  val typeCode: String?,
  val typeDescription: String?,
)
