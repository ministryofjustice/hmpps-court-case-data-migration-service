package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp

data class Person(
  val id: Int,
  val title: String?,
  val firstName: String?,
  val middleName: String?,
  val lastName: String?,
  val dateOfBirth: Timestamp?,
  val nationalId: Int?,
  val nationalityCode: String?,
  val nationalityDescription: String?,
  val additionalNationalityId: Int?,
  val additionalNationalityDescription: String?,
  val disabilityStatus: String?,
  val sex: String?,
  val nationalInsuranceNumber: String?,
  val occupation: String?,
  val occupationCode: String?,
  val ethnicity: String?,
  val contactInformation: String?,
  val address: String?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
