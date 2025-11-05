package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.config.job

import org.slf4j.LoggerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.SkipListener
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.batch.core.launch.support.RunIdIncrementer
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider
import org.springframework.batch.item.database.JdbcBatchItemWriter
import org.springframework.batch.item.database.JdbcCursorItemReader
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder
import org.springframework.batch.item.support.builder.CompositeItemProcessorBuilder
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.PlatformTransactionManager
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.config.BatchProperties
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.HearingConstants
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.JobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.HearingQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Hearing
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.RowCountListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.TimerJobListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.HearingProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.JobScheduler
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.SchedulingConfigRepository
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.JobService
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.HearingValidator
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.PostMigrationValidator
import javax.sql.DataSource

@Configuration
@EnableBatchProcessing
class HearingBatchConfig(
  private val jobRepository: JobRepository,
  private val transactionManager: PlatformTransactionManager,
  @Qualifier("sourceDataSource") private val sourceDataSource: DataSource,
  @Qualifier("targetDataSource") private val targetDataSource: DataSource,
  private val batchProperties: BatchProperties,
) {

  private val log = LoggerFactory.getLogger(HearingBatchConfig::class.java)

  @Autowired
  lateinit var jobLauncher: JobLauncher

  @Autowired
  @Qualifier("sourceJdbcTemplate")
  lateinit var sourceJdbcTemplate: JdbcTemplate

  @Bean
  @StepScope
  fun hearingReader(
    @Value("#{jobParameters['minId']}") minId: Long?,
    @Value("#{jobParameters['maxId']}") maxId: Long?,
  ): JdbcCursorItemReader<HearingQueryResult> = JdbcCursorItemReaderBuilder<HearingQueryResult>()
    .name("hearingReader")
    .dataSource(sourceDataSource)
    .fetchSize(3000)
    .sql("${HearingConstants.SOURCE_QUERY} WHERE h.id BETWEEN $minId AND $maxId ORDER BY h.id ASC")
    .rowMapper { rs, _ ->
      HearingQueryResult(
        id = rs.getInt("id"),
        hearingType = rs.getString("hearing_type"),
        hearingEventType = rs.getString("hearing_event_type"),
        listNo = rs.getString("list_no"),
        firstCreated = rs.getTimestamp("first_created"),
        hearingOutcomes = rs.getString("hearing_outcomes"),
        hearingNotes = rs.getString("hearing_notes"),
        created = rs.getTimestamp("created"),
        createdBy = rs.getString("created_by"),
        lastUpdated = rs.getTimestamp("last_updated"),
        lastUpdatedBy = rs.getString("last_updated_by"),
        deleted = rs.getBoolean("deleted"),
        version = rs.getInt("version"),
      )
    }
    .build()

  @Bean
  fun hearingProcessor(): ItemProcessor<HearingQueryResult, Hearing> = CompositeItemProcessorBuilder<HearingQueryResult, Hearing>()
    .delegates(listOf(HearingProcessor()))
    .build()

  @Bean
  fun hearingWriter(): JdbcBatchItemWriter<Hearing> = JdbcBatchItemWriterBuilder<Hearing>()
    .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider())
    .sql(
      """INSERT INTO hmpps_court_case_service.hearing (id, type, event_type, list_number, first_created, hearing_outcome, hearing_case_note, created_at, created_by, updated_at, updated_by, is_deleted, version)
        VALUES (:id, :type, :eventType, :listNumber, :firstCreated, CAST(:hearingOutcome AS jsonb), CAST(:hearingCaseNote AS jsonb), :createdAt, :createdBy, :updatedAt, :updatedBy, :isDeleted, :version)""",
    )
    .dataSource(targetDataSource)
    .build()

  @Bean
  fun hearingSkipListener() = object : SkipListener<HearingQueryResult, Hearing> {
    override fun onSkipInRead(t: Throwable) {
      log.warn("Skipped during read: ${t.message}")
    }

    override fun onSkipInProcess(item: HearingQueryResult, t: Throwable) {
      log.warn("Skipped during process: ${item.id}, reason: ${t.message}")
    }

    override fun onSkipInWrite(item: Hearing, t: Throwable) {
      log.warn("Skipped during write: ${item.id}, reason: ${t.message}")
    }
  }

  @Bean
  fun hearingStep(): Step = StepBuilder("hearingStep", jobRepository)
    .chunk<HearingQueryResult, Hearing>(batchProperties.chunkSize, transactionManager)
    .reader(hearingReader(null, null))
    .processor(hearingProcessor())
    .writer(hearingWriter())
    .listener(hearingSkipListener())
    .faultTolerant()
    .retry(Throwable::class.java)
    .retryLimit(3)
    .build()

  @Bean
  fun hearingRowCountListener(): RowCountListener = RowCountListener(
    sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
    targetJdbcTemplate = JdbcTemplate(targetDataSource),
    sourceRowCountQuery = HearingConstants.SOURCE_ROW_COUNT_QUERY,
    targetRowCountQuery = HearingConstants.TARGET_ROW_COUNT_QUERY,
  )

  fun validationStep(): Step = StepBuilder("validationStep", jobRepository)
    .tasklet(validationTasklet(), transactionManager)
    .build()

  fun validationTasklet(): Tasklet {
    val strategy = HearingValidator(
      sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
      targetJdbcTemplate = JdbcTemplate(targetDataSource),
    )
    return PostMigrationValidator(strategy, 100)
  }

  @Bean
  fun hearingJob(timerJobListener: TimerJobListener): Job = JobBuilder("hearingJob", jobRepository)
    .incrementer(RunIdIncrementer())
    .listener(timerJobListener)
    .listener(hearingRowCountListener())
    .start(hearingStep())
    .next(validationStep())
    .build()

  @Bean(name = ["hearingJobService"])
  fun hearingJobService(@Qualifier("hearingJob") hearingJob: Job): JobService = JobService(
    jobLauncher = jobLauncher,
    job = hearingJob,
    sourceJdbcTemplate = sourceJdbcTemplate,
    batchSize = 15,
    minQuery = HearingConstants.MIN_QUERY,
    maxQuery = HearingConstants.MAX_QUERY,
    jobName = "Hearing",
  )

  @Bean
  fun hearingJobScheduler(dataSource: DataSource, timerJobListener: TimerJobListener) = JobScheduler(
    jobService = hearingJobService(hearingJob(timerJobListener)),
    jobType = JobType.HEARING,
    schedulingConfigRepository = SchedulingConfigRepository(JdbcTemplate(dataSource)),
  )
}
