package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import com.github.f4b6a3.uuid.UuidCreator
import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.OffenderMatchQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.OffenderMatch

class OffenderMatchProcessor : ItemProcessor<OffenderMatchQueryResult, OffenderMatch> {

  override fun process(offenderMatchQueryResult: OffenderMatchQueryResult): OffenderMatch = OffenderMatch(
    id = UuidCreator.getTimeOrderedEpochPlus1(),
    legacyID = offenderMatchQueryResult.id.toLong(),
    offenderID = null,
    legacyOffenderID = offenderMatchQueryResult.fkOffenderId,
    offenderMatchGroupID = null,
    legacyOffenderMatchGroupID = offenderMatchQueryResult.groupId,
    matchType = offenderMatchQueryResult.matchType,
    aliases = offenderMatchQueryResult.aliases,
    isRejected = offenderMatchQueryResult.rejected,
    matchProbability = offenderMatchQueryResult.matchProbability,
    createdAt = offenderMatchQueryResult.created,
    createdBy = offenderMatchQueryResult.createdBy,
    updatedAt = offenderMatchQueryResult.lastUpdated,
    updatedBy = offenderMatchQueryResult.lastUpdatedBy,
    isDeleted = offenderMatchQueryResult.deleted,
    version = offenderMatchQueryResult.version,
  )
}
