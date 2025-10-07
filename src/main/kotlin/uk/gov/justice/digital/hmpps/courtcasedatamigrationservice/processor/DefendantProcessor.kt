package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.Address
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.DefendantQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.Name
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.PhoneNumber
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.ContactInformation
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Defendant
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Person
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Sex
import kotlin.String
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Address as TargetAddress

class DefendantProcessor : ItemProcessor<DefendantQueryResult, Defendant> {

  private val log = LoggerFactory.getLogger(DefendantProcessor::class.java)

  private val objectMapper = jacksonObjectMapper().apply {
    registerModule(JavaTimeModule())
  }

  override fun process(defendantQueryResult: DefendantQueryResult): Defendant {
    log.info("Processing defendant with ID: {}", defendantQueryResult.id)

    return Defendant(
      id = defendantQueryResult.id,
      isManualUpdate = defendantQueryResult.isManualUpdate,
      crn = defendantQueryResult.crn,
      croNumber = defendantQueryResult.cro,
      tsvName = defendantQueryResult.tsvName,
      pncId = defendantQueryResult.pnc,
      cprUuid = defendantQueryResult.cpr_uuid,
      isOffenderConfirmed = defendantQueryResult.offenderConfirmed,
      person = buildPersonJSONBString(defendantQueryResult),
      createdAt = defendantQueryResult.created,
      createdBy = defendantQueryResult.createdBy,
      updatedAt = defendantQueryResult.lastUpdated,
      updatedBy = defendantQueryResult.lastUpdatedBy,
      isDeleted = defendantQueryResult.deleted,
      version = defendantQueryResult.version,
    )
  }

  private fun buildPersonJSONBString(defendantQueryResult: DefendantQueryResult): String {
    val name: Name? = defendantQueryResult.name?.let {
      try {
        objectMapper.readValue(it)
      } catch (ex: Exception) {
        log.warn("Failed to deserialize name for defendant ID: ${defendantQueryResult.id}", ex)
        null
      }
    }

    val person = Person(
      id = defendantQueryResult.id,
      title = name?.title,
      firstName = name?.forename1,
      middleName = name?.forename2,
      lastName = name?.surname,
      dateOfBirth = defendantQueryResult.dateOfBirth,
      nationalId = null,
      nationalityCode = null,
      nationalityDescription = defendantQueryResult.nationality1,
      additionalNationalityId = null,
      additionalNationalityDescription = defendantQueryResult.nationality2,
      disabilityStatus = null,
      sex = buildSexObject(defendantQueryResult),
      nationalInsuranceNumber = null,
      occupation = null,
      occupationCode = null,
      contactInformation = buildContactInformationObject(defendantQueryResult),
      address = buildAddressObject(defendantQueryResult),
      // TODO check these fields for person
      createdAt = defendantQueryResult.created,
      createdBy = defendantQueryResult.createdBy,
      updatedAt = defendantQueryResult.lastUpdated,
      updatedBy = defendantQueryResult.lastUpdatedBy,
      isDeleted = defendantQueryResult.deleted,
      version = defendantQueryResult.version,
    )
    return objectMapper.writeValueAsString(person)
  }

  private fun buildAddressObject(defendantQueryResult: DefendantQueryResult): TargetAddress {
    val address: Address? = defendantQueryResult.address?.let {
      try {
        objectMapper.readValue(it)
      } catch (ex: Exception) {
        log.warn("Failed to deserialize address for defendant ID: ${defendantQueryResult.id}", ex)
        null
      }
    }

    return TargetAddress(
      id = defendantQueryResult.id, // TODO what do we do about the ids here?
      address1 = address?.line1,
      address2 = address?.line2,
      address3 = address?.line3,
      address4 = address?.line4,
      address5 = address?.line5,
      postcode = address?.postcode,
      createdAt = null,
      createdBy = null,
      updatedAt = null,
      updatedBy = null,
      isDeleted = null,
      version = null,
    )
  }

  private fun buildContactInformationObject(defendantQueryResult: DefendantQueryResult): ContactInformation {
    val phoneNumber: PhoneNumber? = defendantQueryResult.phoneNumber?.let {
      try {
        objectMapper.readValue(it)
      } catch (ex: Exception) {
        log.warn("Failed to deserialize name for defendant ID: ${defendantQueryResult.id}", ex)
        null
      }
    }

    return ContactInformation(
      id = defendantQueryResult.id, // TODO what do we do about the ids here?
      homeNumber = phoneNumber?.home,
      workNumber = phoneNumber?.work,
      mobileNumber = phoneNumber?.mobile,
      primaryEmail = null,
      secondaryEmail = null,
      fax = null,
      createdAt = null,
      createdBy = null,
      updatedAt = null,
      updatedBy = null,
      isDeleted = null,
      version = null,
    )
  }

  private fun buildSexObject(defendantQueryResult: DefendantQueryResult): Sex? {
    defendantQueryResult.sex?.let {
      try {
        return Sex(it, null) // TODO should the sex object have an id with the created etc fields too?
      } catch (ex: Exception) {
        log.warn("Failed to deserialize sex for defendant ID: ${defendantQueryResult.id}", ex)
        null
      }
    }
    return null
  }
}
