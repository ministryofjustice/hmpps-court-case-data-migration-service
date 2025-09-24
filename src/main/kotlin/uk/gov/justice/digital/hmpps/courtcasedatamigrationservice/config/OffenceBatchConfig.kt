package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.config

import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.launch.support.RunIdIncrementer
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider
import org.springframework.batch.item.database.JdbcBatchItemWriter
import org.springframework.batch.item.database.JdbcCursorItemReader
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder
import org.springframework.batch.item.support.builder.CompositeItemProcessorBuilder
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.transaction.PlatformTransactionManager
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.OffenceQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Offence
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.OffenceJobListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.OffenceProcessor
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

  @Bean
  fun reader(): JdbcCursorItemReader<OffenceQueryResult> = JdbcCursorItemReaderBuilder<OffenceQueryResult>()
    .name("offenceReader")
    .dataSource(sourceDataSource)
    .sql(
      """select o.id, o.fk_hearing_defendant_id, o.offence_code, o.summary, o.title, o.sequence, o.act, o.list_no, o.short_term_custody_predictor_score, o.created, o.created_by, o.last_updated, o.last_updated_by, o.deleted, o.version, 
p.id plea_id, p.date plea_date, p.value plea_value, p.created plea_created, p.created_by plea_created_by, p.last_updated plea_last_updated, p.last_updated_by plea_last_updated_by, p.deleted plea_deleted, p.version plea_version, 
v."id" verdict_id, v."date" verdict_date, v.type_description verdict_type_description, v.created verdict_created, v.created_by verdict_created_by, v.last_updated verdict_last_updated, v.last_updated_by verdict_last_updated_by, v.deleted verdict_deleted, v.version verdict_version  
from 	courtcaseservice.offence o left join courtcaseservice.plea p on (o.plea_id = p.id)
		left join courtcaseservice.verdict v on (o.verdict_id = v.id)
order by o.id"""
        .trimMargin(),
    )
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
        pleaId = rs.getObject("plea_id", Integer::class.java),
        pleaDate = rs.getTimestamp("plea_date"),
        pleaValue = rs.getString("plea_value"),
        pleaCreated = rs.getTimestamp("plea_created"),
        pleaCreatedBy = rs.getString("plea_created_by"),
        pleaLastUpdated = rs.getTimestamp("plea_last_updated"),
        pleaLastUpdatedBy = rs.getString("plea_last_updated_by"),
        pleaDeleted = rs.getBoolean("plea_deleted"),
        pleaVersion = rs.getInt("plea_version"),
        verdictId = rs.getObject("verdict_id", Integer::class.java),
        verdictDate = rs.getTimestamp("verdict_date"),
        verdictTypeDescription = rs.getString("verdict_type_description"),
        verdictCreated = rs.getTimestamp("verdict_created"),
        verdictCreatedBy = rs.getString("verdict_created_by"),
        verdictLastUpdated = rs.getTimestamp("verdict_last_updated"),
        verdictLastUpdatedBy = rs.getString("verdict_last_updated_by"),
        verdictDeleted = rs.getBoolean("verdict_deleted"),
        verdictVersion = rs.getInt("verdict_version"),
      )
    }
    .build()

  @Bean
  fun processor(): ItemProcessor<OffenceQueryResult, Offence> = CompositeItemProcessorBuilder<OffenceQueryResult, Offence>()
    .delegates(listOf(OffenceProcessor()))
    .build()

  @Bean
  fun writer(): JdbcBatchItemWriter<Offence> = JdbcBatchItemWriterBuilder<Offence>()
    .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider())
    .sql(
      """INSERT INTO hmpps_court_case_service.offence (id, code, title, act, list_number, sequence, facts, is_discontinued, short_term_custody_predictor_score, plea, verdict, created_at, created_by, updated_at, updated_by, is_deleted, version)
        VALUES (:id, :code, :title, :act, :list_number, :sequence, :facts, :isDiscontinued, :shortTermCustodyPredictorScore, CAST(:plea AS jsonb), CAST(:verdict AS jsonb), :createdAt, :createdBy, :updatedAt, :updatedBy, :isDeleted, :version)""",
    )
    .dataSource(targetDataSource)
    .build()

  @Bean
  fun offenceStep(): Step = StepBuilder("offenceStep", jobRepository)
    .chunk<OffenceQueryResult, Offence>(batchProperties.chunkSize, transactionManager)
    .reader(reader())
    .processor(processor())
    .writer(writer())
    .build()

  @Bean
  fun job(offenceJobListener: OffenceJobListener): Job = JobBuilder("offenceJob", jobRepository)
    .incrementer(RunIdIncrementer())
    .listener(offenceJobListener)
    .start(offenceStep())
    .build()
}
