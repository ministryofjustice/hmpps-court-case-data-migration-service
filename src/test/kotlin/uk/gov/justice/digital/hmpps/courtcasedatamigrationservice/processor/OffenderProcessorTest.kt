package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.OffenderQueryResult
import java.sql.Date
import java.sql.Timestamp
import java.time.LocalDate
import java.time.Month

class OffenderProcessorTest {

  private lateinit var objectMapper: ObjectMapper
  private lateinit var processor: OffenderProcessor

  @BeforeEach
  fun setup() {
    objectMapper = ObjectMapper().registerModule(KotlinModule.Builder().build())
    processor = OffenderProcessor()
  }

  @Test
  fun `should map offenderQueryResult to offender`() {
    val offenderQueryResult = OffenderQueryResult(
      id = 1,
      suspendedSentenceOrder = true,
      breach = false,
      awaitingPSR = true,
      probationStatus = "CURRENT",
      preSentenceActivity = true,
      previouslyKnownTerminationDate = Date.valueOf(LocalDate.of(2015, Month.SEPTEMBER, 15)),
      created = Timestamp.valueOf("2025-09-24 12:00:00"),
      createdBy = "system",
      lastUpdated = Timestamp.valueOf("2025-09-24 12:30:00"),
      lastUpdatedBy = "system",
      deleted = false,
      version = 1,
    )

    val offender = processor.process(offenderQueryResult)

    assertThat(offender.id).isEqualTo(1)
    assertThat(offender.suspendedSentenceOrder).isEqualTo(true)
    assertThat(offender.breach).isEqualTo(false)
    assertThat(offender.awaitingPSR).isEqualTo(true)
    assertThat(offender.probationStatus).isEqualTo("CURRENT")
    assertThat(offender.preSentenceActivity).isEqualTo(true)
    assertThat(offender.previouslyKnownTerminationDate).isEqualTo(Date.valueOf(LocalDate.of(2015, Month.SEPTEMBER, 15)))
    assertThat(offender.createdAt).isEqualTo(Timestamp.valueOf("2025-09-24 12:00:00"))
    assertThat(offender.createdBy).isEqualTo("system")
    assertThat(offender.updatedAt).isEqualTo(Timestamp.valueOf("2025-09-24 12:30:00"))
    assertThat(offender.updatedBy).isEqualTo("system")
    assertThat(offender.isDeleted).isFalse()
    assertThat(offender.version).isEqualTo(1)
  }
}
