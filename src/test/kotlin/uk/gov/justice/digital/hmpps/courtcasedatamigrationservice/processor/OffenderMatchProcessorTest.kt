package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.OffenderMatchQueryResult
import java.sql.Timestamp

class OffenderMatchProcessorTest {

  private lateinit var objectMapper: ObjectMapper
  private lateinit var processor: OffenderMatchProcessor

  @BeforeEach
  fun setup() {
    objectMapper = ObjectMapper().registerModule(KotlinModule.Builder().build())
    processor = OffenderMatchProcessor()
  }

  @Test
  fun `should map offenderMatchQueryResult to offenderMatch`() {
    val offenderMatchQueryResult = OffenderMatchQueryResult(
      id = 1,
      fkOffenderId = 100L,
      groupId = 150L,
      matchType = "NAME_DOB_PNC",
      aliases = """[{"gender": "Male", "surname": "Magnusson", "firstName": "Victor", "dateOfBirth": "1997-02-28", "middleNames": []}]""",
      rejected = true,
      matchProbability = null, // TODO implement this in test
      created = Timestamp.valueOf("2025-09-24 12:00:00"),
      createdBy = "system",
      lastUpdated = Timestamp.valueOf("2025-09-24 12:30:00"),
      lastUpdatedBy = "system",
      deleted = false,
      version = 1,
    )

    val offenderMatch = processor.process(offenderMatchQueryResult)

    assertThat(offenderMatch.legacyID).isEqualTo(1)
    assertThat(offenderMatch.offenderID).isNull()
    assertThat(offenderMatch.offenderMatchGroupID).isNull()
    assertThat(offenderMatch.matchType).isEqualTo("NAME_DOB_PNC")
    assertThat(offenderMatch.aliases).isEqualTo("""[{"gender": "Male", "surname": "Magnusson", "firstName": "Victor", "dateOfBirth": "1997-02-28", "middleNames": []}]""")
    assertThat(offenderMatch.isRejected).isEqualTo(true)
    assertThat(offenderMatch.matchProbability).isNull()
    assertThat(offenderMatch.createdAt).isEqualTo(Timestamp.valueOf("2025-09-24 12:00:00"))
    assertThat(offenderMatch.createdBy).isEqualTo("system")
    assertThat(offenderMatch.updatedAt).isEqualTo(Timestamp.valueOf("2025-09-24 12:30:00"))
    assertThat(offenderMatch.updatedBy).isEqualTo("system")
    assertThat(offenderMatch.isDeleted).isFalse()
    assertThat(offenderMatch.version).isEqualTo(1)
  }
}
