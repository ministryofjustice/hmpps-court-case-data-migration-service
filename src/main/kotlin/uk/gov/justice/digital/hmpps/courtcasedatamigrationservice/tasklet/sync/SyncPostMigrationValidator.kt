package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.sync

import org.slf4j.LoggerFactory
import org.springframework.batch.core.StepContribution
import org.springframework.batch.core.scope.context.ChunkContext
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.repeat.RepeatStatus

class SyncPostMigrationValidator(
  private val validator: SyncValidator,
  private val sampleSize: Int = 10,
) : Tasklet {

  private val log = LoggerFactory.getLogger(SyncPostMigrationValidator::class.java)

  override fun execute(
    contribution: StepContribution,
    chunkContext: ChunkContext,
  ): RepeatStatus = validateSample(chunkContext.stepContext.jobParameters)

  fun validateSample(jobParameters: Map<String, Any>): RepeatStatus {
    log.info("Validating sample IDs from source data")

    val minId = jobParameters["minId"] as? Long
    val maxId = jobParameters["maxId"] as? Long

    if (minId == null || maxId == null || minId > maxId) {
      throw RuntimeException("Invalid ID range. Stopping further job executions.")
    }

    val sampleSourceIDs = validator.fetchSourceIDs(minId, maxId, sampleSize)

    log.info("Sample size: {}", sampleSourceIDs.size)
    log.info("Sample source IDs: {}", sampleSourceIDs.joinToString(", "))

    val errors = mutableListOf<String>()

    sampleSourceIDs.forEach { id ->
      val source = validator.fetchSourceRecord(id)

      if (source != null) {
        val target = validator.fetchTargetRecord(source)
        if (target != null && target["count"] == 1L) {
          return@forEach
        }
      }

      errors.add("$source not found in target database.")
    }

    if (errors.isEmpty()) {
      log.info("✅ All sample records passed validation.")
    } else {
      log.error("❌ Validation errors found:\n${errors.joinToString("\n")}. Stopping further job executions.")
      throw RuntimeException("Validation failed for job with parameters: $jobParameters")
    }

    return RepeatStatus.FINISHED
  }
}
