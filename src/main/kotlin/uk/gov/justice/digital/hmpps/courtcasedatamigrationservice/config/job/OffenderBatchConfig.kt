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
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderConstants.MAX_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderConstants.MIN_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderConstants.SOURCE_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderConstants.SOURCE_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderConstants.TARGET_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.JobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.OffenderQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Offender
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.RowCountListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.TimerJobListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.OffenderProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.JobScheduler
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.SchedulingConfigRepository
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.JobService
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.OffenderValidator
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.PostMigrationValidator
import javax.sql.DataSource

@Configuration
@EnableBatchProcessing
class OffenderBatchConfig(
  private val jobRepository: JobRepository,
  private val transactionManager: PlatformTransactionManager,
  @Qualifier("sourceDataSource") private val sourceDataSource: DataSource,
  @Qualifier("targetDataSource") private val targetDataSource: DataSource,
  private val batchProperties: BatchProperties,
) {

  private val log = LoggerFactory.getLogger(OffenderBatchConfig::class.java)

  @Autowired
  lateinit var jobLauncher: JobLauncher

  @Bean
  @StepScope
  fun offenderReader(
    @Value("#{jobParameters['minId']}") minId: Long?,
    @Value("#{jobParameters['maxId']}") maxId: Long?,
  ): JdbcCursorItemReader<OffenderQueryResult> = JdbcCursorItemReaderBuilder<OffenderQueryResult>()
    .name("offenderReader")
    .dataSource(sourceDataSource)
    .fetchSize(3000)
    .sql("${SOURCE_QUERY} WHERE o.id BETWEEN $minId AND $maxId order by id asc")
    .rowMapper { rs, _ ->
      OffenderQueryResult(
        id = rs.getInt("id"),
        suspendedSentenceOrder = rs.getBoolean("suspended_sentence_order"),
        breach = rs.getBoolean("breach"),
        awaitingPSR = rs.getBoolean("awaiting_psr"),
        probationStatus = rs.getString("probation_status"),
        preSentenceActivity = rs.getBoolean("pre_sentence_activity"),
        previouslyKnownTerminationDate = rs.getDate("previously_known_termination_date"),
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
  fun offenderProcessor(): ItemProcessor<OffenderQueryResult, Offender> = CompositeItemProcessorBuilder<OffenderQueryResult, Offender>()
    .delegates(listOf(OffenderProcessor()))
    .build()

  @Bean
  fun offenderWriter(): JdbcBatchItemWriter<Offender> = JdbcBatchItemWriterBuilder<Offender>()
    .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider())
    .sql(
      """INSERT INTO hmpps_court_case_service.offender (id, legacy_id, suspended_sentence_order, breach, awaiting_psr, probation_status, pre_sentence_activity, previously_known_termination_date, created_at, created_by, updated_at, updated_by, is_deleted, version)
        VALUES (:id, :legacyID, :suspendedSentenceOrder, :breach, :awaitingPSR, :probationStatus, :preSentenceActivity, :previouslyKnownTerminationDate, :createdAt, :createdBy, :updatedAt, :updatedBy, :isDeleted, :version)""",
    )
    .dataSource(targetDataSource)
    .build()

  @Bean
  fun offenderSkipListener() = object : SkipListener<OffenderQueryResult, Offender> {
    override fun onSkipInRead(t: Throwable) {
      log.warn("Skipped during read: ${t.message}")
    }

    override fun onSkipInProcess(item: OffenderQueryResult, t: Throwable) {
      log.warn("Skipped during process: ${item.id}, reason: ${t.message}")
    }

    override fun onSkipInWrite(item: Offender, t: Throwable) {
      log.warn("Skipped during write: ${item.id}, reason: ${t.message}")
    }
  }

  @Bean
  fun offenderStep(): Step = StepBuilder("offenderStep", jobRepository)
    .chunk<OffenderQueryResult, Offender>(batchProperties.chunkSize, transactionManager)
    .reader(offenderReader(null, null))
    .processor(offenderProcessor())
    .writer(offenderWriter())
    .listener(offenderSkipListener())
    .faultTolerant()
    .retry(Throwable::class.java)
    .retryLimit(3)
    .build()

  @Bean
  fun offenderRowCountListener(): RowCountListener = RowCountListener(
    sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
    targetJdbcTemplate = JdbcTemplate(targetDataSource),
    sourceRowCountQuery = SOURCE_ROW_COUNT_QUERY,
    targetRowCountQuery = TARGET_ROW_COUNT_QUERY,
  )

  fun validationStep(): Step = StepBuilder("validationStep", jobRepository)
    .tasklet(validationTasklet(), transactionManager)
    .build()

  fun validationTasklet(): Tasklet {
    val strategy = OffenderValidator(
      sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
      targetJdbcTemplate = JdbcTemplate(targetDataSource),
    )
    return PostMigrationValidator(strategy, 100)
  }

  @Bean
  fun offenderJob(timerJobListener: TimerJobListener): Job = JobBuilder("offenderJob", jobRepository)
    .incrementer(RunIdIncrementer())
    .listener(timerJobListener)
    .listener(offenderRowCountListener())
    .start(offenderStep())
    .next(validationStep())
    .build()

  @Bean(name = ["offenderJobService"])
  fun offenderJobService(@Qualifier("offenderJob") offenderJob: Job): JobService = JobService(
    jobLauncher = jobLauncher,
    job = offenderJob,
    jdbcTemplate = JdbcTemplate(sourceDataSource),
    batchSize = 15,
    minQuery = MIN_QUERY,
    maxQuery = MAX_QUERY,
    jobName = "Offender",
  )

  @Bean
  fun offenderJobScheduler(dataSource: DataSource, timerJobListener: TimerJobListener) = JobScheduler(
    jobService = offenderJobService(offenderJob(timerJobListener)),
    jobType = JobType.OFFENDER,
    schedulingConfigRepository = SchedulingConfigRepository(JdbcTemplate(dataSource)),
  )
}
