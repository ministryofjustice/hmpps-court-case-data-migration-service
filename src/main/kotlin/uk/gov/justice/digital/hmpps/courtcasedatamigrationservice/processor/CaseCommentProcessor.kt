package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import com.github.f4b6a3.uuid.UuidCreator
import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.CaseCommentQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.CaseComment

class CaseCommentProcessor : ItemProcessor<CaseCommentQueryResult, CaseComment> {

  override fun process(caseCommentQueryResult: CaseCommentQueryResult): CaseComment = CaseComment(
    id = UuidCreator.getTimeOrderedEpochPlus1(),
    legacyID = caseCommentQueryResult.id.toLong(),
    defendantID = null,
    legacyDefendantID = caseCommentQueryResult.defendantID,
    caseID = null,
    legacyCaseID = caseCommentQueryResult.caseID,
    author = caseCommentQueryResult.author,
    comment = caseCommentQueryResult.comment,
    isDraft = caseCommentQueryResult.isDraft,
    isLegacy = caseCommentQueryResult.legacy,
    createdAt = caseCommentQueryResult.created,
    createdBy = caseCommentQueryResult.createdBy,
    updatedAt = caseCommentQueryResult.lastUpdated,
    updatedBy = caseCommentQueryResult.lastUpdatedBy,
    isDeleted = caseCommentQueryResult.deleted,
    version = caseCommentQueryResult.version,

  )
}
