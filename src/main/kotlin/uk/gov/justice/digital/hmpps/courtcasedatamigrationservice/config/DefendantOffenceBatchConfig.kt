package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.config

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
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantOffenceConstants.MAX_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantOffenceConstants.MIN_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantOffenceConstants.SOURCE_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantOffenceConstants.SOURCE_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantOffenceConstants.TARGET_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.JobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.DefendantOffenceQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.HearingQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.OffenceQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.DefendantOffence
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Hearing
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Offence
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.RowCountListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.TimerJobListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.DefendantOffenceProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.HearingProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.OffenceProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.JobScheduler
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.JobService
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.DefendantOffenceValidator
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.HearingValidator
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.OffenceValidator
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.PostMigrationValidator
import javax.sql.DataSource

@Configuration
@EnableBatchProcessing
class DefendantOffenceBatchConfig(
  private val jobRepository: JobRepository,
  private val transactionManager: PlatformTransactionManager,
  @Qualifier("sourceDataSource") private val sourceDataSource: DataSource,
  @Qualifier("targetDataSource") private val targetDataSource: DataSource,
  private val batchProperties: BatchProperties,
) {

  private val log = LoggerFactory.getLogger(DefendantOffenceBatchConfig::class.java)

  @Autowired
  lateinit var jobLauncher: JobLauncher

  @Autowired
  @Qualifier("sourceJdbcTemplate")
  lateinit var sourceJdbcTemplate: JdbcTemplate

  @Bean
  @StepScope
  fun defendantOffenceReader(
    @Value("#{jobParameters['minId']}") minId: Long?,
    @Value("#{jobParameters['maxId']}") maxId: Long?,
  ): JdbcCursorItemReader<DefendantOffenceQueryResult> = JdbcCursorItemReaderBuilder<DefendantOffenceQueryResult>()
    .name("defendantOffenceReader")
    .dataSource(sourceDataSource)
    .fetchSize(3000)
    .sql("$SOURCE_QUERY WHERE o.id BETWEEN $minId AND $maxId ORDER BY o.id ASC")
    .rowMapper { rs, _ ->
      DefendantOffenceQueryResult(
        id = rs.getInt("id"),
        offenceId = rs.getInt("offence_id"),
        defendantId = rs.getInt("defendant_id"),
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
  fun defendantOffenceProcessor(): ItemProcessor<DefendantOffenceQueryResult, DefendantOffence> = CompositeItemProcessorBuilder<DefendantOffenceQueryResult, DefendantOffence>()
    .delegates(listOf(DefendantOffenceProcessor()))
    .build()

  @Bean
  fun defendantOffenceWriter(): JdbcBatchItemWriter<DefendantOffence> = JdbcBatchItemWriterBuilder<DefendantOffence>()
    .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider())
    .sql(
      """INSERT INTO hmpps_court_case_service.defendant_offence (id, offence_id, defendant_id, created_at, created_by, updated_at, updated_by, is_deleted, version)
        VALUES (:id, :offenceId, :defendantId, :createdAt, :createdBy, :updatedAt, :updatedBy, :isDeleted, :version)""",
    )
    .dataSource(targetDataSource)
    .build()

  @Bean
  fun defendantOffenceSkipListener() = object : SkipListener<DefendantOffenceQueryResult, DefendantOffence> {
    override fun onSkipInRead(t: Throwable) {
      log.warn("Skipped during read: ${t.message}")
    }

    override fun onSkipInProcess(item: DefendantOffenceQueryResult, t: Throwable) {
      log.warn("Skipped during process: ${item.id}, reason: ${t.message}")
    }

    override fun onSkipInWrite(item: DefendantOffence, t: Throwable) {
      log.warn("Skipped during write: ${item.id}, reason: ${t.message}")
    }
  }

  @Bean
  fun defendantOffenceStep(): Step = StepBuilder("defendantOffenceStep", jobRepository)
    .chunk<DefendantOffenceQueryResult, DefendantOffence>(batchProperties.chunkSize, transactionManager)
    .reader(defendantOffenceReader(null, null))
    .processor(defendantOffenceProcessor())
    .writer(defendantOffenceWriter())
    .listener(defendantOffenceSkipListener())
    .faultTolerant()
    .retry(Throwable::class.java)
    .retryLimit(3)
    .build()

  @Bean
  fun defendantOffenceRowCountListener(): RowCountListener = RowCountListener(
    sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
    targetJdbcTemplate = JdbcTemplate(targetDataSource),
    sourceRowCountQuery = SOURCE_ROW_COUNT_QUERY,
    targetRowCountQuery = TARGET_ROW_COUNT_QUERY,
  )

  fun validationStep(): Step = StepBuilder("validationStep", jobRepository)
    .tasklet(validationTasklet(), transactionManager)
    .build()

  fun validationTasklet(): Tasklet {
    val strategy = DefendantOffenceValidator(
      sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
      targetJdbcTemplate = JdbcTemplate(targetDataSource),
    )
    return PostMigrationValidator(strategy, 100)
  }

  @Bean
  fun defendantOffenceJob(timerJobListener: TimerJobListener): Job = JobBuilder("defendantOffenceJob", jobRepository)
    .incrementer(RunIdIncrementer())
    .listener(timerJobListener)
    .listener(defendantOffenceRowCountListener())
    .start(defendantOffenceStep())
    .next(validationStep())
    .build()

  @Bean(name = ["defendantOffenceJobService"])
  fun defendantOffenceJobService(@Qualifier("defendantOffenceJob") defendantOffenceJob: Job): JobService = JobService(
    jobLauncher = jobLauncher,
    job = defendantOffenceJob,
    sourceJdbcTemplate = sourceJdbcTemplate,
    batchSize = 5,
    minQuery = MIN_QUERY,
    maxQuery = MAX_QUERY,
    jobName = "DefendantOffence",
  )

  @Bean
  fun defendantOffenceJobScheduler(dataSource: DataSource, timerJobListener: TimerJobListener) = JobScheduler(jobService = defendantOffenceJobService(defendantOffenceJob(timerJobListener)), dataSource = dataSource, jobType = JobType.DEFENDANT_OFFENCE)
}
