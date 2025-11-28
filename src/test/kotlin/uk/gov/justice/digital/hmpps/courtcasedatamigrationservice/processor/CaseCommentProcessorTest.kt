package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.TestUtils.isValueUUID
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.CaseCommentQueryResult
import java.sql.Timestamp
import java.util.UUID

class CaseCommentProcessorTest {

  private lateinit var objectMapper: ObjectMapper
  private lateinit var processor: CaseCommentProcessor

  @BeforeEach
  fun setup() {
    objectMapper = ObjectMapper().registerModule(KotlinModule.Builder().build())
    processor = CaseCommentProcessor()
  }

  @Test
  fun `should map caseCommentQueryResult to caseComment`() {
    val caseCommentQueryResult = CaseCommentQueryResult(
      id = 1,
      caseID = "5fe72014-6ee4-4412-be14-c1a86ff33baf",
      defendantID = UUID.fromString("95f4f9d8-6057-48d4-92aa-0e452f0e38d7"),
      author = "Mike Pitt",
      comment = "This is a comment.",
      isDraft = true,
      legacy = true,
      created = Timestamp.valueOf("2025-09-24 12:00:00"),
      createdBy = "system",
      lastUpdated = Timestamp.valueOf("2025-09-24 12:30:00"),
      lastUpdatedBy = "system",
      deleted = false,
      version = 1,
    )

    val caseComment = processor.process(caseCommentQueryResult)

    assertThat { isValueUUID(caseComment.id.toString()) }
    assertThat(caseComment.legacyID).isEqualTo(1)
    assertThat(caseComment.defendantID).isNull()
    assertThat(caseComment.legacyDefendantID.toString()).isEqualTo("95f4f9d8-6057-48d4-92aa-0e452f0e38d7")
    assertThat(caseComment.caseID).isNull()
    assertThat(caseComment.legacyCaseID.toString()).isEqualTo("5fe72014-6ee4-4412-be14-c1a86ff33baf")
    assertThat(caseComment.author).isEqualTo("Mike Pitt")
    assertThat(caseComment.comment).isEqualTo("This is a comment.")
    assertThat(caseComment.isDraft).isEqualTo(true)
    assertThat(caseComment.isLegacy).isEqualTo(true)
    assertThat(caseComment.createdAt).isEqualTo(Timestamp.valueOf("2025-09-24 12:00:00"))
    assertThat(caseComment.createdBy).isEqualTo("system")
    assertThat(caseComment.updatedAt).isEqualTo(Timestamp.valueOf("2025-09-24 12:30:00"))
    assertThat(caseComment.updatedBy).isEqualTo("system")
    assertThat(caseComment.isDeleted).isFalse()
    assertThat(caseComment.version).isEqualTo(1)
  }
}
