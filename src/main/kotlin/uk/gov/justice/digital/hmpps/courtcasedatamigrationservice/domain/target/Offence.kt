package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp
import java.util.UUID

// TODO add wording / summary to the offence job
data class Offence(
  val id: UUID,
  val legacyID: Long?,
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
