package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

data class Offence(
  val id: Int,
  val title: String,
  val plea: String?
  )
