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
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchGroupConstants.MAX_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchGroupConstants.MIN_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchGroupConstants.SOURCE_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchGroupConstants.SOURCE_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchGroupConstants.TARGET_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.JobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.OffenderMatchGroupQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.OffenderMatchGroup
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.RowCountListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.TimerJobListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.OffenderMatchGroupProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.JobScheduler
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.SchedulingConfigRepository
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.JobService
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.OffenderMatchGroupValidator
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.PostMigrationValidator
import javax.sql.DataSource

@Configuration
@EnableBatchProcessing
class OffenderMatchGroupBatchConfig(
  private val jobRepository: JobRepository,
  private val transactionManager: PlatformTransactionManager,
  @Qualifier("sourceDataSource") private val sourceDataSource: DataSource,
  @Qualifier("targetDataSource") private val targetDataSource: DataSource,
  private val batchProperties: BatchProperties,
) {

  private val log = LoggerFactory.getLogger(OffenderMatchGroupBatchConfig::class.java)

  @Autowired
  lateinit var jobLauncher: JobLauncher

  @Autowired
  @Qualifier("sourceJdbcTemplate")
  lateinit var sourceJdbcTemplate: JdbcTemplate

  @Bean
  @StepScope
  fun offenderMatchGroupReader(
    @Value("#{jobParameters['minId']}") minId: Long?,
    @Value("#{jobParameters['maxId']}") maxId: Long?,
  ): JdbcCursorItemReader<OffenderMatchGroupQueryResult> = JdbcCursorItemReaderBuilder<OffenderMatchGroupQueryResult>()
    .name("offenderMatchGroupReader")
    .dataSource(sourceDataSource)
    .fetchSize(3000)
    .sql("$SOURCE_QUERY WHERE omg.id BETWEEN $minId AND $maxId order by omg.id")
    .rowMapper { rs, _ ->
      OffenderMatchGroupQueryResult(
        id = rs.getInt("id"),
        caseId = rs.getObject("case_id", Integer::class.java)?.toInt(),
        defendantId = rs.getObject("defendant_id", Integer::class.java)?.toInt(),
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
  fun offenderMatchGroupProcessor(): ItemProcessor<OffenderMatchGroupQueryResult, OffenderMatchGroup> = CompositeItemProcessorBuilder<OffenderMatchGroupQueryResult, OffenderMatchGroup>()
    .delegates(listOf(OffenderMatchGroupProcessor()))
    .build()

  @Bean
  fun offenderMatchGroupWriter(): JdbcBatchItemWriter<OffenderMatchGroup> = JdbcBatchItemWriterBuilder<OffenderMatchGroup>()
    .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider())
    .sql(
      """INSERT INTO hmpps_court_case_service.offender_match_group (id, defendant_id, prosecution_case_id, created_at, created_by, updated_at, updated_by, is_deleted, version)
        VALUES (:id, :defendantId, :prosecutionCaseId, :createdAt, :createdBy, :updatedAt, :updatedBy, :isDeleted, :version)""",
    )
    .dataSource(targetDataSource)
    .build()

  @Bean
  fun offenderMatchGroupSkipListener() = object : SkipListener<OffenderMatchGroupQueryResult, OffenderMatchGroup> {
    override fun onSkipInRead(t: Throwable) {
      log.warn("Skipped during read: ${t.message}")
    }

    override fun onSkipInProcess(item: OffenderMatchGroupQueryResult, t: Throwable) {
      log.warn("Skipped during process: ${item.id}, reason: ${t.message}")
    }

    override fun onSkipInWrite(item: OffenderMatchGroup, t: Throwable) {
      log.warn("Skipped during write: ${item.id}, reason: ${t.message}")
    }
  }

  @Bean
  fun offenderMatchGroupStep(): Step = StepBuilder("offenderMatchGroupStep", jobRepository)
    .chunk<OffenderMatchGroupQueryResult, OffenderMatchGroup>(batchProperties.chunkSize, transactionManager)
    .reader(offenderMatchGroupReader(null, null))
    .processor(offenderMatchGroupProcessor())
    .writer(offenderMatchGroupWriter())
    .listener(offenderMatchGroupSkipListener())
    .faultTolerant()
    .retry(Throwable::class.java)
    .retryLimit(3)
    .build()

  @Bean
  fun offenderMatchGroupRowCountListener(): RowCountListener = RowCountListener(
    sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
    targetJdbcTemplate = JdbcTemplate(targetDataSource),
    sourceRowCountQuery = SOURCE_ROW_COUNT_QUERY,
    targetRowCountQuery = TARGET_ROW_COUNT_QUERY,
  )

  fun validationStep(): Step = StepBuilder("validationStep", jobRepository)
    .tasklet(validationTasklet(), transactionManager)
    .build()

  fun validationTasklet(): Tasklet {
    val strategy = OffenderMatchGroupValidator(
      sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
      targetJdbcTemplate = JdbcTemplate(targetDataSource),
    )
    return PostMigrationValidator(strategy, 100)
  }

  @Bean
  fun offenderMatchGroupJob(timerJobListener: TimerJobListener): Job = JobBuilder(
    "offenderMatchGroupJob",
    jobRepository,
  )
    .incrementer(RunIdIncrementer())
    .listener(timerJobListener)
    .listener(offenderMatchGroupRowCountListener())
    .start(offenderMatchGroupStep())
    .next(validationStep())
    .build()

  @Bean(name = ["offenderMatchGroupJobService"])
  fun offenderMatchGroupJobService(@Qualifier("offenderMatchGroupJob") offenderMatchGroupJob: Job): JobService = JobService(
    jobLauncher = jobLauncher,
    job = offenderMatchGroupJob,
    sourceJdbcTemplate = sourceJdbcTemplate,
    batchSize = 15,
    minQuery = MIN_QUERY,
    maxQuery = MAX_QUERY,
    jobName = "OffenderMatchGroup",
  )

  @Bean
  fun offenderMatchGroupJobScheduler(dataSource: DataSource, timerJobListener: TimerJobListener) = JobScheduler(
    jobService = offenderMatchGroupJobService(offenderMatchGroupJob(timerJobListener)),
    jobType = JobType.OFFENDER_MATCH_GROUP,
    schedulingConfigRepository = SchedulingConfigRepository(JdbcTemplate(dataSource)),
  )
}
