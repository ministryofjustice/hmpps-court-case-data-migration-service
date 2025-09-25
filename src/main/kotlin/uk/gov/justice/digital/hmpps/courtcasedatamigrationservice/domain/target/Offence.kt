package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp

data class Offence(
  val id: Int,
  val code: String?,
  val title: String?,
  val act: String?,
  val list_number: Int?,
  val sequence: Int?,
  val facts: String?,
  val isDiscontinued: Boolean?,
  val shortTermCustodyPredictorScore: Int?,
  val judicialResult: String?,
  val plea: String?,
  val verdict: String?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
