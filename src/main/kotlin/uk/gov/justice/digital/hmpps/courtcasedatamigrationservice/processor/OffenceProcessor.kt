package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.OffenceQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Offence as TargetOffence
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Plea as TargetPlea

class OffenceProcessor : ItemProcessor<OffenceQueryResult, TargetOffence> {

  private companion object {
    val log: Logger = LoggerFactory.getLogger(this::class.java)
  }

  private val objectMapper = jacksonObjectMapper()

  override fun process(offenceQueryResult: OffenceQueryResult): TargetOffence {
    log.info("Processing offence: {}", offenceQueryResult.id)

    val targetPlea = if (offenceQueryResult.pleaId != null) {
      objectMapper.writeValueAsString(TargetPlea(offenceQueryResult.pleaId, offenceQueryResult.pleaValue))
    } else {
      null
    }

    val targetOffence = TargetOffence(
      id = offenceQueryResult.id,
      title = offenceQueryResult.title,
      plea = targetPlea,
    )

    return targetOffence
  }
}
