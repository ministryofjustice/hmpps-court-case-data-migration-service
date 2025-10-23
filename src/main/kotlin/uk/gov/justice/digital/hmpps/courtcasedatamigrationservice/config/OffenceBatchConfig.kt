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
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenceConstants.MAX_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenceConstants.MIN_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenceConstants.SOURCE_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenceConstants.SOURCE_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenceConstants.TARGET_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.JobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.OffenceQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Offence
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.RowCountListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.TimerJobListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.OffenceProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.JobScheduler
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.JobService
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.OffenceValidator
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.PostMigrationValidator
import javax.sql.DataSource

@Configuration
@EnableBatchProcessing
class OffenceBatchConfig(
  private val jobRepository: JobRepository,
  private val transactionManager: PlatformTransactionManager,
  @Qualifier("sourceDataSource") private val sourceDataSource: DataSource,
  @Qualifier("targetDataSource") private val targetDataSource: DataSource,
  private val batchProperties: BatchProperties,
) {

  private val log = LoggerFactory.getLogger(OffenceBatchConfig::class.java)

  @Autowired
  lateinit var jobLauncher: JobLauncher

  @Autowired
  @Qualifier("sourceJdbcTemplate")
  lateinit var sourceJdbcTemplate: JdbcTemplate

  @Bean
  @StepScope
  fun offenceReader(
    @Value("#{jobParameters['minId']}") minId: Long?,
    @Value("#{jobParameters['maxId']}") maxId: Long?,
  ): JdbcCursorItemReader<OffenceQueryResult> = JdbcCursorItemReaderBuilder<OffenceQueryResult>()
    .name("offenceReader")
    .dataSource(sourceDataSource)
    .fetchSize(3000)
    .sql("$SOURCE_QUERY WHERE o.id BETWEEN $minId AND $maxId")
    .rowMapper { rs, _ ->
      OffenceQueryResult(
        id = rs.getInt("id"),
        fkHearingDefendantId = rs.getLong("fk_hearing_defendant_id"),
        summary = rs.getString("summary"),
        title = rs.getString("title"),
        offenceCode = rs.getString("offence_code"),
        sequence = rs.getInt("sequence"),
        act = rs.getString("act"),
        listNo = rs.getInt("list_no"),
        shortTeamCustodyPredictorScore = rs.getInt("short_term_custody_predictor_score"),
        created = rs.getTimestamp("created"),
        createdBy = rs.getString("created_by"),
        lastUpdated = rs.getTimestamp("last_updated"),
        lastUpdatedBy = rs.getString("last_updated_by"),
        deleted = rs.getBoolean("deleted"),
        version = rs.getInt("version"),
        pleaId = rs.getObject("plea_id", Integer::class.java)?.toInt(),
        pleaDate = rs.getTimestamp("plea_date"),
        pleaValue = rs.getString("plea_value"),
        pleaCreated = rs.getTimestamp("plea_created"),
        pleaCreatedBy = rs.getString("plea_created_by"),
        pleaLastUpdated = rs.getTimestamp("plea_last_updated"),
        pleaLastUpdatedBy = rs.getString("plea_last_updated_by"),
        pleaDeleted = rs.getBoolean("plea_deleted"),
        pleaVersion = rs.getInt("plea_version"),
        verdictId = rs.getObject("verdict_id", Integer::class.java)?.toInt(),
        verdictDate = rs.getTimestamp("verdict_date"),
        verdictTypeDescription = rs.getString("verdict_type_description"),
        verdictCreated = rs.getTimestamp("verdict_created"),
        verdictCreatedBy = rs.getString("verdict_created_by"),
        verdictLastUpdated = rs.getTimestamp("verdict_last_updated"),
        verdictLastUpdatedBy = rs.getString("verdict_last_updated_by"),
        verdictDeleted = rs.getBoolean("verdict_deleted"),
        verdictVersion = rs.getInt("verdict_version"),
        judicialResults = rs.getString("judicial_results"),
      )
    }
    .build()

  @Bean
  fun offenceProcessor(): ItemProcessor<OffenceQueryResult, Offence> = CompositeItemProcessorBuilder<OffenceQueryResult, Offence>()
    .delegates(listOf(OffenceProcessor()))
    .build()

  @Bean
  fun offenceWriter(): JdbcBatchItemWriter<Offence> = JdbcBatchItemWriterBuilder<Offence>()
    .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider())
    .sql(
      """INSERT INTO hmpps_court_case_service.offence (id, code, title, legislation, listing_number, sequence, short_term_custody_predictor_score, wording, plea, verdict, judicial_results, created_at, created_by, updated_at, updated_by, is_deleted, version)
        VALUES (:id, :code, :title, :legislation, :listingNumber, :sequence, :shortTermCustodyPredictorScore, :wording, CAST(:plea AS jsonb), CAST(:verdict AS jsonb), CAST(:judicialResults AS jsonb), :createdAt, :createdBy, :updatedAt, :updatedBy, :isDeleted, :version)""",
    )
    .dataSource(targetDataSource)
    .build()

  @Bean
  fun offenceSkipListener() = object : SkipListener<OffenceQueryResult, Offence> {
    override fun onSkipInRead(t: Throwable) {
      log.warn("Skipped during read: ${t.message}")
    }

    override fun onSkipInProcess(item: OffenceQueryResult, t: Throwable) {
      log.warn("Skipped during process: ${item.id}, reason: ${t.message}")
    }

    override fun onSkipInWrite(item: Offence, t: Throwable) {
      log.warn("Skipped during write: ${item.id}, reason: ${t.message}")
    }
  }

  @Bean
  fun offenceStep(): Step = StepBuilder("offenceStep", jobRepository)
    .chunk<OffenceQueryResult, Offence>(batchProperties.chunkSize, transactionManager)
    .reader(offenceReader(null, null))
    .processor(offenceProcessor())
    .writer(offenceWriter())
    .listener(offenceSkipListener())
    .faultTolerant()
    .retry(Throwable::class.java)
    .retryLimit(3)
    .build()

  @Bean
  fun offenceRowCountListener(): RowCountListener = RowCountListener(
    sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
    targetJdbcTemplate = JdbcTemplate(targetDataSource),
    sourceRowCountQuery = SOURCE_ROW_COUNT_QUERY,
    targetRowCountQuery = TARGET_ROW_COUNT_QUERY,
  )

  fun validationStep(): Step = StepBuilder("validationStep", jobRepository)
    .tasklet(validationTasklet(), transactionManager)
    .build()

  fun validationTasklet(): Tasklet {
    val strategy = OffenceValidator(
      sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
      targetJdbcTemplate = JdbcTemplate(targetDataSource),
    )
    return PostMigrationValidator(strategy, 100)
  }

  @Bean
  fun offenceJob(timerJobListener: TimerJobListener): Job = JobBuilder("offenceJob", jobRepository)
    .incrementer(RunIdIncrementer())
    .listener(timerJobListener)
    .listener(offenceRowCountListener())
    .start(offenceStep())
    .next(validationStep())
    .build()

  @Bean(name = ["offenceJobService"])
  fun offenceJobService(@Qualifier("offenceJob") offenceJob: Job): JobService = JobService(
    jobLauncher = jobLauncher,
    job = offenceJob,
    sourceJdbcTemplate = sourceJdbcTemplate,
    batchSize = 15,
    minQuery = MIN_QUERY,
    maxQuery = MAX_QUERY,
    jobName = "Offence",
  )

  @Bean
  fun offenceJobScheduler(dataSource: DataSource, timerJobListener: TimerJobListener) = JobScheduler(jobService = offenceJobService(offenceJob(timerJobListener)), dataSource = dataSource, jobType = JobType.OFFENCE)
}
