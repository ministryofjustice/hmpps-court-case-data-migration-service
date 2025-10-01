package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.OffenceQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.JudicialResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Plea
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Verdict
import java.sql.Timestamp

class OffenceProcessorTest {

  private lateinit var objectMapper: ObjectMapper
  private lateinit var processor: OffenceProcessor

  @BeforeEach
  fun setup() {
    objectMapper = ObjectMapper().registerModule(KotlinModule.Builder().build())
    processor = OffenceProcessor()
  }

  @Test
  fun `should map offenceQueryResult to offence`() {
    val offenceQueryResult = OffenceQueryResult(
      id = 1,
      fkHearingDefendantId = 2L,
      offenceCode = "ABC123",
      summary = "this is summary",
      title = "this is title",
      sequence = 1,
      act = "this is act",
      listNo = 1,
      shortTeamCustodyPredictorScore = 1,
      created = Timestamp.valueOf("2025-09-24 12:00:00"),
      createdBy = "system",
      lastUpdated = Timestamp.valueOf("2025-09-24 12:30:00"),
      lastUpdatedBy = "system",
      deleted = false,
      version = 1,
      pleaId = 101,
      pleaDate = Timestamp.valueOf("2025-09-23 10:00:00"),
      pleaValue = "Guilty",
      pleaCreated = Timestamp.valueOf("2025-09-23 10:05:00"),
      pleaCreatedBy = "clerk",
      pleaLastUpdated = Timestamp.valueOf("2025-09-23 10:10:00"),
      pleaLastUpdatedBy = "clerk",
      pleaDeleted = false,
      pleaVersion = 1,
      verdictId = 201,
      verdictDate = Timestamp.valueOf("2025-09-24 14:00:00"),
      verdictTypeDescription = "Convicted",
      verdictCreated = Timestamp.valueOf("2025-09-24 14:05:00"),
      verdictCreatedBy = "judge",
      verdictLastUpdated = Timestamp.valueOf("2025-09-24 14:10:00"),
      verdictLastUpdatedBy = "judge",
      verdictDeleted = false,
      verdictVersion = 1,
      judicialResults = """[{"id" : 59222, "is_convicted_result" : false, "judicial_result_type_id" : "705140dc-833a-4aa0-a872-839009fc4494", "label" : "Sent to Crown Court for trial on unconditional bail", "result_text" : null, "created" : "2022-10-04T12:09:46.003856", "created_by" : "(court-case-matcher)", "last_updated" : null, "last_updated_by" : null, "deleted" : false, "version" : 0}]""",
    )

    val offence = processor.process(offenceQueryResult)

    assertThat(offence.id).isEqualTo(1)
    assertThat(offence.code).isEqualTo("ABC123")
    assertThat(offence.title).isEqualTo("this is title")
    assertThat(offence.sequence).isEqualTo(1)
    assertThat(offence.legislation).isEqualTo("this is act")
    assertThat(offence.listingNumber).isEqualTo(1)
    assertThat(offence.shortTermCustodyPredictorScore).isEqualTo(1)
    assertThat(offence.createdAt).isEqualTo(Timestamp.valueOf("2025-09-24 12:00:00"))
    assertThat(offence.createdBy).isEqualTo("system")
    assertThat(offence.updatedAt).isEqualTo(Timestamp.valueOf("2025-09-24 12:30:00"))
    assertThat(offence.updatedBy).isEqualTo("system")
    assertThat(offence.isDeleted).isFalse()
    assertThat(offence.version).isEqualTo(1)

    val plea: Plea = objectMapper.readValue(offence.plea, Plea::class.java)

    assertThat(plea.id).isEqualTo(101)
    assertThat(plea.date).isEqualTo(Timestamp.valueOf("2025-09-23 10:00:00"))
    assertThat(plea.value).isEqualTo("Guilty")
    assertThat(plea.createdAt).isEqualTo(Timestamp.valueOf("2025-09-23 10:05:00"))
    assertThat(plea.createdBy).isEqualTo("clerk")
    assertThat(plea.lastUpdatedAt).isEqualTo(Timestamp.valueOf("2025-09-23 10:10:00"))
    assertThat(plea.lastUpdatedBy).isEqualTo("clerk")
    assertThat(plea.isDeleted).isFalse()
    assertThat(plea.version).isEqualTo(1)

    val verdict: Verdict = objectMapper.readValue(offence.verdict, Verdict::class.java)

    assertThat(verdict.id).isEqualTo(201)
    assertThat(verdict.date).isEqualTo(Timestamp.valueOf("2025-09-24 14:00:00"))
    assertThat(verdict.type).isEqualTo("Convicted")
    assertThat(verdict.createdAt).isEqualTo(Timestamp.valueOf("2025-09-24 14:05:00"))
    assertThat(verdict.createdBy).isEqualTo("judge")
    assertThat(verdict.lastUpdatedAt).isEqualTo(Timestamp.valueOf("2025-09-24 14:10:00"))
    assertThat(verdict.lastUpdatedBy).isEqualTo("judge")
    assertThat(verdict.isDeleted).isFalse()
    assertThat(verdict.version).isEqualTo(1)

    val judicialResults: List<JudicialResult> = objectMapper.readValue(
      offence.judicialResults,
      object : TypeReference<List<JudicialResult>>() {},
    )

    assertThat(judicialResults.size).isEqualTo(1)
    assertThat(judicialResults[0].id).isEqualTo(59222)
    assertThat(judicialResults[0].isConvictedResult).isEqualTo(false)
    assertThat(judicialResults[0].label).isEqualTo("Sent to Crown Court for trial on unconditional bail")
    assertThat(judicialResults[0].resultTypeId).isEqualTo("705140dc-833a-4aa0-a872-839009fc4494")
    assertThat(judicialResults[0].resultText).isNull()
    assertThat(judicialResults[0].isJudicialResultDeleted).isNull()
    assertThat(judicialResults[0].createdBy).isEqualTo("(court-case-matcher)")
    assertThat(judicialResults[0].createdAt).isEqualTo("2022-10-04T12:09:46.003856")
    assertThat(judicialResults[0].updatedBy).isNull()
    assertThat(judicialResults[0].updatedAt).isNull()
    assertThat(judicialResults[0].isDeleted).isFalse()
    assertThat(judicialResults[0].version).isEqualTo(0)
  }
}
