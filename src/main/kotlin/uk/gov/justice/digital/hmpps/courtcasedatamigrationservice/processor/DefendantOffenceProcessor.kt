package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.DefendantOffenceQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.DefendantOffence

class DefendantOffenceProcessor : ItemProcessor<DefendantOffenceQueryResult, DefendantOffence> {

  override fun process(defendantOffenceQueryResult: DefendantOffenceQueryResult): DefendantOffence = DefendantOffence(
    id = defendantOffenceQueryResult.id,
    offenceId = defendantOffenceQueryResult.offenceId,
    defendantId = defendantOffenceQueryResult.defendantId,
    createdAt = defendantOffenceQueryResult.created,
    createdBy = defendantOffenceQueryResult.createdBy,
    updatedAt = defendantOffenceQueryResult.lastUpdated,
    updatedBy = defendantOffenceQueryResult.lastUpdatedBy,
    isDeleted = defendantOffenceQueryResult.deleted,
    version = defendantOffenceQueryResult.version,
  )
}
