package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.DefendantQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Defendant

class DefendantProcessor : ItemProcessor<DefendantQueryResult, Defendant> {

  private companion object {
    val log: Logger = LoggerFactory.getLogger(this::class.java)
  }

  override fun process(defendantQueryResult: DefendantQueryResult): Defendant {
    log.info("Processing defendant: {}", defendantQueryResult.id)

    return Defendant(
      id = defendantQueryResult.id,
      masterDefendantId = null,
      numberOfPreviousConvictionsCited = null,
      isManualUpdate = null,
      mitigation = null,
      crn = null,
      croNumber = null,
      isYouth = null,
      tsvName = null,
      pncId = null,
      isProceedingsConcluded = null,
      cprUuid = null,
      isOffenderConfirmed = null,
      person = null,
      createdAt = null,
      createdBy = null,
      updatedAt = null,
      updatedBy = null,
      isDeleted = null,
      version = null,
    )
  }
}
