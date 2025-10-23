package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp

// TODO add wording / summary to the offence job
data class Offence(
  val id: Int,
  val code: String?,
  val title: String?,
  val legislation: String?,
  val listingNumber: Int?,
  val sequence: Int?,
  val shortTermCustodyPredictorScore: Int?,
  val wording: String?,
  val judicialResults: String?,
  val plea: String?,
  val verdict: String?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
