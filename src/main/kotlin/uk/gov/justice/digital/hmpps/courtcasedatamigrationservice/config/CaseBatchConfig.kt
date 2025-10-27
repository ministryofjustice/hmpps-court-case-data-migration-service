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
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.CaseConstants.MAX_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.CaseConstants.MIN_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.CaseConstants.SOURCE_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.CaseConstants.SOURCE_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.CaseConstants.TARGET_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.JobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.CaseQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.ProsecutionCase
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.RowCountListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.TimerJobListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.CaseProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.JobScheduler
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.SchedulingConfigRepository
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.JobService
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.CaseValidator
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.PostMigrationValidator
import javax.sql.DataSource

@Configuration
@EnableBatchProcessing
class CaseBatchConfig(
  private val jobRepository: JobRepository,
  private val transactionManager: PlatformTransactionManager,
  @Qualifier("sourceDataSource") private val sourceDataSource: DataSource,
  @Qualifier("targetDataSource") private val targetDataSource: DataSource,
  private val batchProperties: BatchProperties,
) {

  private val log = LoggerFactory.getLogger(CaseBatchConfig::class.java)

  @Autowired
  lateinit var jobLauncher: JobLauncher

  @Autowired
  @Qualifier("sourceJdbcTemplate")
  lateinit var sourceJdbcTemplate: JdbcTemplate

  @Bean
  @StepScope
  fun caseReader(
    @Value("#{jobParameters['minId']}") minId: Long?,
    @Value("#{jobParameters['maxId']}") maxId: Long?,
  ): JdbcCursorItemReader<CaseQueryResult> = JdbcCursorItemReaderBuilder<CaseQueryResult>()
    .name("caseReader")
    .dataSource(sourceDataSource)
    .fetchSize(3000)
    .sql("$SOURCE_QUERY WHERE cc.id BETWEEN $minId AND $maxId")
    .rowMapper { rs, _ ->
      CaseQueryResult(
        id = rs.getInt("id"),
        caseId = rs.getString("case_id"),
        urn = rs.getString("urn"),
        sourceType = rs.getString("source_type"),
        created = rs.getTimestamp("created"),
        createdBy = rs.getString("created_by"),
        lastUpdated = rs.getTimestamp("last_updated"),
        lastUpdatedBy = rs.getString("last_updated_by"),
        deleted = rs.getBoolean("deleted"),
        version = rs.getInt("version"),
        caseDocuments = rs.getString("case_documents"),
        caseMarkers = rs.getString("case_markers"),
      )
    }
    .build()

  @Bean
  fun caseProcessor(): ItemProcessor<CaseQueryResult, ProsecutionCase> = CompositeItemProcessorBuilder<CaseQueryResult, ProsecutionCase>()
    .delegates(listOf(CaseProcessor()))
    .build()

  @Bean
  fun caseWriter(): JdbcBatchItemWriter<ProsecutionCase> = JdbcBatchItemWriterBuilder<ProsecutionCase>()
    .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider())
    .sql(
      """INSERT INTO hmpps_court_case_service.prosecution_case (id, case_id,  case_urn, source_type, c_id, case_markers, case_documents, created_at, created_by, updated_at, updated_by, is_deleted, version)
        VALUES (:id, :caseId, CAST(:caseURN AS jsonb), :sourceType, :cID, CAST(:caseMarkers AS jsonb), CAST(:caseDocuments AS jsonb), :createdAt, :createdBy, :updatedAt, :updatedBy, :isDeleted, :version)""",
    )
    .dataSource(targetDataSource)
    .build()

  @Bean
  fun caseSkipListener() = object : SkipListener<CaseQueryResult, ProsecutionCase> {
    override fun onSkipInRead(t: Throwable) {
      log.warn("Skipped during read: ${t.message}")
    }

    override fun onSkipInProcess(item: CaseQueryResult, t: Throwable) {
      log.warn("Skipped during process: ${item.id}, reason: ${t.message}")
    }

    override fun onSkipInWrite(item: ProsecutionCase, t: Throwable) {
      log.warn("Skipped during write: ${item.id}, reason: ${t.message}")
    }
  }

  @Bean
  fun caseStep(): Step = StepBuilder("caseStep", jobRepository)
    .chunk<CaseQueryResult, ProsecutionCase>(batchProperties.chunkSize, transactionManager)
    .reader(caseReader(null, null))
    .processor(caseProcessor())
    .writer(caseWriter())
    .listener(caseSkipListener())
    .faultTolerant()
    .retry(Throwable::class.java)
    .retryLimit(3)
    .build()

  @Bean
  fun caseRowCountListener(): RowCountListener = RowCountListener(
    sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
    targetJdbcTemplate = JdbcTemplate(targetDataSource),
    sourceRowCountQuery = SOURCE_ROW_COUNT_QUERY,
    targetRowCountQuery = TARGET_ROW_COUNT_QUERY,
  )

  fun validationStep(): Step = StepBuilder("validationStep", jobRepository)
    .tasklet(validationTasklet(), transactionManager)
    .build()

  fun validationTasklet(): Tasklet {
    val strategy = CaseValidator(
      sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
      targetJdbcTemplate = JdbcTemplate(targetDataSource),
    )
    return PostMigrationValidator(strategy, 100)
  }

  @Bean
  fun caseJob(timerJobListener: TimerJobListener): Job = JobBuilder("caseJob", jobRepository)
    .incrementer(RunIdIncrementer())
    .listener(timerJobListener)
    .listener(caseRowCountListener())
    .start(caseStep())
    .next(validationStep())
    .build()

  @Bean(name = ["caseJobService"])
  fun caseJobService(@Qualifier("caseJob") caseJob: Job): JobService = JobService(
    jobLauncher = jobLauncher,
    job = caseJob,
    sourceJdbcTemplate = sourceJdbcTemplate,
    batchSize = 5,
    minQuery = MIN_QUERY,
    maxQuery = MAX_QUERY,
    jobName = "Case",
  )

  @Bean
  fun caseJobScheduler(dataSource: DataSource, timerJobListener: TimerJobListener) = JobScheduler(
    jobService = caseJobService(caseJob(timerJobListener)),
    jobType = JobType.CASE,
    schedulingConfigRepository = SchedulingConfigRepository(JdbcTemplate(dataSource)),
  )
}
