package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source

data class Offence(
  val id: Int,
  val fkHearingDefendantId: Long,
  val summary: String,
  val title: String,
  val sequence: Long,
  val act: String?,
  val pleaId: Int
)