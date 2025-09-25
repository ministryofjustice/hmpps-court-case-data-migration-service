package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp

data class ProsecutionCase(
  val id: Int,
  val caseURN: String?,
  val sourceType: String?,
  val typeId: String?,
  val typeCode: String?,
  val typeDescription: String?,
  val caseDocument: String?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
