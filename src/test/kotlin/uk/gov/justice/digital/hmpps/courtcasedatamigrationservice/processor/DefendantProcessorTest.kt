package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.DefendantQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.ContactInformation
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Person
import java.sql.Timestamp
import java.time.LocalDate
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Address as TargetAddress

class DefendantProcessorTest {

  private lateinit var objectMapper: ObjectMapper
  private lateinit var processor: DefendantProcessor

  @BeforeEach
  fun setup() {
    objectMapper = jacksonObjectMapper().apply {
      registerModule(JavaTimeModule())
    }
    processor = DefendantProcessor()
  }

  @Test
  fun `should map defendantQueryResult to defendant`() {
    val defendantQueryResult = DefendantQueryResult(
      id = 123,
      name = """{"title":"Ms","forename1":"Jane","forename2":"A.","surname":"Smith"}""",
      address = """{"line1":"123 Main St","line2":"Flat 4","postcode":"AB12 3CD"}""",
      phoneNumber = """{"home":"01234567890","work":"01111222333","mobile":"07700900000"}""",
      dateOfBirth = LocalDate.of(1990, 1, 1),
      nationality1 = "British",
      nationality2 = "Irish",
      sex = "F",
      isManualUpdate = false,
      crn = "CRN456",
      cro = "CRO789",
      tsvName = "Jane Smith",
      pnc = "PNC123456",
      cprUUID = "UUID-1234",
      offenderConfirmed = true,
      fkOffenderId = 10,
      created = Timestamp.valueOf("2025-09-24 12:00:00"),
      createdBy = "system",
      lastUpdated = Timestamp.valueOf("2025-09-24 12:30:00"),
      lastUpdatedBy = "system",
      deleted = false,
      version = 1,
    )

    val defendant = processor.process(defendantQueryResult)

    assertThat(defendant.id).isEqualTo(123)
    assertThat(defendant.crn).isEqualTo("CRN456")
    assertThat(defendant.tsvName).isEqualTo("Jane Smith")
    assertThat(defendant.pncId).isEqualTo("PNC123456")
    assertThat(defendant.offenderId).isEqualTo(10)
    assertThat(defendant.createdAt).isEqualTo(Timestamp.valueOf("2025-09-24 12:00:00"))
    assertThat(defendant.updatedAt).isEqualTo(Timestamp.valueOf("2025-09-24 12:30:00"))
    assertThat(defendant.isDeleted).isFalse()
    assertThat(defendant.version).isEqualTo(1)

    val person: Person? = defendant.person?.let { objectMapper.readValue(it) }

    assertThat(person).isNotNull()
    assertThat(person!!.id).isEqualTo(123)
    assertThat(person.title).isEqualTo("Ms")
    assertThat(person.firstName).isEqualTo("Jane")
    assertThat(person.middleName).isEqualTo("A.")
    assertThat(person.lastName).isEqualTo("Smith")
    assertThat(person.dateOfBirth).isEqualTo(LocalDate.of(1990, 1, 1))
    assertThat(person.nationalityDescription).isEqualTo("British")
    assertThat(person.additionalNationalityDescription).isEqualTo("Irish")
    assertThat(person.sex?.code).isEqualTo("F")

    val address: TargetAddress = person.address!!
    assertThat(address.address1).isEqualTo("123 Main St")
    assertThat(address.address2).isEqualTo("Flat 4")
    assertThat(address.postcode).isEqualTo("AB12 3CD")

    val contact: ContactInformation = person.contactInformation!!
    assertThat(contact.homeNumber).isEqualTo("01234567890")
    assertThat(contact.workNumber).isEqualTo("01111222333")
    assertThat(contact.mobileNumber).isEqualTo("07700900000")
  }
}
