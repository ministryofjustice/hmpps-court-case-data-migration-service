package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.util.UUID

data class Person(
  val id: UUID,
  var title: String?,
  val firstName: String?,
  val middleName: String?,
  val lastName: String?,
  val dateOfBirth: String?,
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
  val createdAt: String?,
  val createdBy: String?,
  val updatedAt: String?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
