package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.CourtQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.CourtRoom
import java.sql.Timestamp

class CourtProcessorTest {

  private lateinit var objectMapper: ObjectMapper
  private lateinit var processor: CourtProcessor

  @BeforeEach
  fun setup() {
    objectMapper = ObjectMapper().registerModule(KotlinModule.Builder().build())
    processor = CourtProcessor()
  }

  @Test
  fun `should map courtQueryResult to courtCentre`() {
    val courtQueryResult = CourtQueryResult(
      id = 1,
      name = "Solihull Magistrates Court",
      courtCode = "B20LW",
      created = Timestamp.valueOf("2025-09-24 12:00:00"),
      createdBy = "system",
      lastUpdated = Timestamp.valueOf("2025-09-24 12:30:00"),
      lastUpdatedBy = "system",
      deleted = false,
      version = 1,
      courtRooms = """[{"id" : 5710550, "court_room" : "05"}, {"id" : 5710532, "court_room" : "06"}, {"id" : 5710530, "court_room" : "07"}]""",
    )

    val courtCentre = processor.process(courtQueryResult)

    assertThat(courtCentre.legacyID).isEqualTo(1)
    assertThat(courtCentre.name).isEqualTo("Solihull Magistrates Court")
    assertThat(courtCentre.code).isEqualTo("B20LW")
    assertThat(courtCentre.createdAt).isEqualTo(Timestamp.valueOf("2025-09-24 12:00:00"))
    assertThat(courtCentre.createdBy).isEqualTo("system")
    assertThat(courtCentre.updatedAt).isEqualTo(Timestamp.valueOf("2025-09-24 12:30:00"))
    assertThat(courtCentre.updatedBy).isEqualTo("system")
    assertThat(courtCentre.isDeleted).isFalse()
    assertThat(courtCentre.version).isEqualTo(1)

    val courtRooms: List<CourtRoom> = objectMapper.readValue(
      courtCentre.courtRooms,
      object : TypeReference<List<CourtRoom>>() {},
    )

    assertThat(courtRooms.size).isEqualTo(3)
    assertThat(courtRooms[0].id).isEqualTo(1)
    assertThat(courtRooms[0].roomName).isEqualTo("05")
    assertThat(courtRooms[1].id).isEqualTo(2)
    assertThat(courtRooms[1].roomName).isEqualTo("06")
    assertThat(courtRooms[2].id).isEqualTo(3)
    assertThat(courtRooms[2].roomName).isEqualTo("07")
  }
}
