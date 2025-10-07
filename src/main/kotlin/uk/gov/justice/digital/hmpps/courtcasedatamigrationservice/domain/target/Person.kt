package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp
import java.time.LocalDate

data class Person(
  val id: Int,
  var title: String?,
  val firstName: String?,
  val middleName: String?,
  val lastName: String?,
  val dateOfBirth: LocalDate?,
  val nationalId: Int?,
  val nationalityCode: String?,
  val nationalityDescription: String?,
  val additionalNationalityId: Int?,
  val additionalNationalityDescription: String?,
  val disabilityStatus: String?,
  val sex: Sex?,
  val nationalInsuranceNumber: String?,
  val occupation: String?,
  val occupationCode: String?,
  val contactInformation: ContactInformation?,
  val address: Address?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
