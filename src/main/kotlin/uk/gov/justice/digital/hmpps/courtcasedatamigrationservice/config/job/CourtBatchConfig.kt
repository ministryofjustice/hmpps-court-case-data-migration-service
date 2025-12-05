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
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.CourtConstants.MAX_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.CourtConstants.MIN_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.CourtConstants.SOURCE_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.CourtConstants.SOURCE_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.CourtConstants.TARGET_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.JobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.CourtQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.CourtCentre
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.RowCountListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.TimerJobListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.CourtProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.JobScheduler
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.SchedulingConfigRepository
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.JobService
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.CourtValidator
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.PostMigrationValidator
import javax.sql.DataSource

@Configuration
@EnableBatchProcessing
class CourtBatchConfig(
  private val jobRepository: JobRepository,
  private val transactionManager: PlatformTransactionManager,
  @Qualifier("sourceDataSource") private val sourceDataSource: DataSource,
  @Qualifier("targetDataSource") private val targetDataSource: DataSource,
  private val batchProperties: BatchProperties,
) {

  private val log = LoggerFactory.getLogger(CourtBatchConfig::class.java)

  @Autowired
  lateinit var jobLauncher: JobLauncher

  @Bean
  @StepScope
  fun courtReader(
    @Value("#{jobParameters['minId']}") minId: Long?,
    @Value("#{jobParameters['maxId']}") maxId: Long?,
  ): JdbcCursorItemReader<CourtQueryResult> = JdbcCursorItemReaderBuilder<CourtQueryResult>()
    .name("courtReader")
    .dataSource(sourceDataSource)
    .fetchSize(3000)
    .sql("${SOURCE_QUERY} WHERE c.id BETWEEN $minId AND $maxId order by c.id asc")
    .rowMapper { rs, _ ->
      CourtQueryResult(
        id = rs.getInt("id"),
        name = rs.getString("name"),
        courtCode = rs.getString("court_code"),
        created = rs.getTimestamp("created"),
        createdBy = rs.getString("created_by"),
        lastUpdated = rs.getTimestamp("last_updated"),
        lastUpdatedBy = rs.getString("last_updated_by"),
        deleted = rs.getBoolean("deleted"),
        version = rs.getInt("version"),
        courtRooms = rs.getString("court_rooms"),
      )
    }
    .build()

  @Bean
  fun courtProcessor(): ItemProcessor<CourtQueryResult, CourtCentre> = CompositeItemProcessorBuilder<CourtQueryResult, CourtCentre>()
    .delegates(listOf(CourtProcessor()))
    .build()

  @Bean
  fun courtWriter(): JdbcBatchItemWriter<CourtCentre> = JdbcBatchItemWriterBuilder<CourtCentre>()
    .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider())
    .sql(
      """INSERT INTO hmpps_court_case_service.court_centre (id, legacy_id, code, name, court_rooms, psa_code, region, address, created_at, created_by, updated_at, updated_by, is_deleted, version)
        VALUES (:id, :legacyID, :code, :name, CAST(:courtRooms AS jsonb), :psaCode, :region, CAST(:address AS jsonb), :createdAt, :createdBy, :updatedAt, :updatedBy, :isDeleted, :version)""",
    )
    .dataSource(targetDataSource)
    .build()

  @Bean
  fun courtSkipListener() = object : SkipListener<CourtQueryResult, CourtCentre> {
    override fun onSkipInRead(t: Throwable) {
      log.warn("Skipped during read: ${t.message}")
    }

    override fun onSkipInProcess(item: CourtQueryResult, t: Throwable) {
      log.warn("Skipped during process: ${item.id}, reason: ${t.message}")
    }

    override fun onSkipInWrite(item: CourtCentre, t: Throwable) {
      log.warn("Skipped during write: ${item.id}, reason: ${t.message}")
    }
  }

  @Bean
  fun courtStep(): Step = StepBuilder("courtStep", jobRepository)
    .chunk<CourtQueryResult, CourtCentre>(batchProperties.chunkSize, transactionManager)
    .reader(courtReader(null, null))
    .processor(courtProcessor())
    .writer(courtWriter())
    .listener(courtSkipListener())
    .faultTolerant()
    .retry(Throwable::class.java)
    .retryLimit(3)
    .build()

  @Bean
  fun courtRowCountListener(): RowCountListener = RowCountListener(
    sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
    targetJdbcTemplate = JdbcTemplate(targetDataSource),
    sourceRowCountQuery = SOURCE_ROW_COUNT_QUERY,
    targetRowCountQuery = TARGET_ROW_COUNT_QUERY,
  )

  fun validationStep(): Step = StepBuilder("validationStep", jobRepository)
    .tasklet(validationTasklet(), transactionManager)
    .build()

  fun validationTasklet(): Tasklet {
    val strategy = CourtValidator(
      sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
      targetJdbcTemplate = JdbcTemplate(targetDataSource),
    )
    return PostMigrationValidator(strategy, 25)
  }

  @Bean
  fun courtJob(timerJobListener: TimerJobListener): Job = JobBuilder("courtJob", jobRepository)
    .incrementer(RunIdIncrementer())
    .listener(timerJobListener)
    .listener(courtRowCountListener())
    .start(courtStep())
    .next(validationStep())
    .build()

  @Bean(name = ["courtJobService"])
  fun courtJobService(@Qualifier("courtJob") courtJob: Job): JobService = JobService(
    jobLauncher = jobLauncher,
    job = courtJob,
    jdbcTemplate = JdbcTemplate(sourceDataSource),
    batchSize = 15,
    minQuery = MIN_QUERY,
    maxQuery = MAX_QUERY,
    jobName = "Court",
  )

  @Bean
  fun courtJobScheduler(dataSource: DataSource, timerJobListener: TimerJobListener) = JobScheduler(
    jobService = courtJobService(courtJob(timerJobListener)),
    jobType = JobType.COURT,
    schedulingConfigRepository = SchedulingConfigRepository(JdbcTemplate(dataSource)),
  )
}
