package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import com.github.f4b6a3.uuid.UuidCreator
import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.DefendantOffenceQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.DefendantOffence

class DefendantOffenceProcessor : ItemProcessor<DefendantOffenceQueryResult, DefendantOffence> {

  override fun process(defendantOffenceQueryResult: DefendantOffenceQueryResult): DefendantOffence = DefendantOffence(
    id = UuidCreator.getTimeOrderedEpochPlus1(),
    offenceID = null,
    defendantID = null,
    legacyDefendantID = defendantOffenceQueryResult.legacyDefendantId?.toLong(),
    legacyOffenceID = defendantOffenceQueryResult.legacyOffenceId?.toLong(),
    createdAt = defendantOffenceQueryResult.created,
    createdBy = defendantOffenceQueryResult.createdBy,
    updatedAt = defendantOffenceQueryResult.lastUpdated,
    updatedBy = defendantOffenceQueryResult.lastUpdatedBy,
    isDeleted = defendantOffenceQueryResult.deleted,
    version = defendantOffenceQueryResult.version,

  )
}
