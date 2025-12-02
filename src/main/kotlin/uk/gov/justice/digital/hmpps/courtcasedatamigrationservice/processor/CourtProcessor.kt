package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.f4b6a3.uuid.UuidCreator
import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.CourtQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.CourtRoom
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.CourtCentre
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.CourtRoom as TargetCourtRoom

class CourtProcessor : ItemProcessor<CourtQueryResult, CourtCentre> {

  private val objectMapper = jacksonObjectMapper()

  override fun process(courtQueryResult: CourtQueryResult): CourtCentre = CourtCentre(
    id = UuidCreator.getTimeOrderedEpochPlus1(),
    legacyID = courtQueryResult.id.toLong(),
    code = courtQueryResult.courtCode,
    name = courtQueryResult.name,
    createdAt = courtQueryResult.created,
    createdBy = courtQueryResult.createdBy,
    updatedAt = courtQueryResult.lastUpdated,
    updatedBy = courtQueryResult.lastUpdatedBy,
    isDeleted = courtQueryResult.deleted,
    version = courtQueryResult.version,
    psaCode = null,
    region = null,
    address = null,
    courtRooms = buildCourtRoomsAsJSONBString(courtQueryResult),
  )

  private fun buildCourtRoomsAsJSONBString(courtQueryResult: CourtQueryResult): String? {
    val courtRooms: List<TargetCourtRoom>? = courtQueryResult.courtRooms?.let { json ->
      val results: List<CourtRoom> = objectMapper.readValue(json)
      results.mapIndexed { index, result ->
        TargetCourtRoom(
          id = (index + 1), // Start at 1
          roomID = null,
          roomName = result.courtRoom,
        )
      }
    }
    return courtRooms?.let { objectMapper.writeValueAsString(it) }
  }
}
