package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.configuration

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
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.PlatformTransactionManager
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.Offence
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.OffenceQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.OffenceJobListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.OffenceProcessor
import javax.sql.DataSource
import kotlin.collections.listOf

@Configuration
@EnableBatchProcessing
class OffenceBatchConfig(private val jobRepository: JobRepository,
                         private val transactionManager: PlatformTransactionManager,
                         @Qualifier("sourceDataSource") private val sourceDataSource: DataSource,
                         @Qualifier("targetDataSource") private val targetDataSource: DataSource,
                         private val batchProperties: BatchProperties) {

  @Bean
  fun reader(): JdbcCursorItemReader<OffenceQueryResult> =
    JdbcCursorItemReaderBuilder<OffenceQueryResult>()
      .name("offenceReader")
      .dataSource(sourceDataSource)
      .sql("""select o.id, o.fk_hearing_defendant_id, o.summary, o.title, o.sequence, o.act, p.id plea_id, p.value plea_value
from courtcaseservice.offence o left join courtcaseservice.plea p on (o.plea_id = p.id) 
order by o.id"""
        .trimMargin())
      .rowMapper { rs, _ ->
        OffenceQueryResult(
          id = rs.getInt("id"),
          fkHearingDefendantId = rs.getLong("fk_hearing_defendant_id"),
          summary = rs.getString("summary"),
          title = rs.getString("title"),
          sequence = rs.getLong("sequence"),
          act = rs.getString("act"),
          pleaId = rs.getObject("plea_id", java.lang.Integer::class.java),
          pleaValue = rs.getString("plea_value"),
        )
      }
      .build()

  @Bean
  fun processor(sourceJdbcTemplate: JdbcTemplate): ItemProcessor<OffenceQueryResult, uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Offence> = CompositeItemProcessorBuilder<OffenceQueryResult, uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Offence>()
    .delegates(listOf(OffenceProcessor(sourceJdbcTemplate)))
    .build()

  @Bean
  fun writer(): JdbcBatchItemWriter<uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Offence> = JdbcBatchItemWriterBuilder<uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Offence>()
    .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider())
    .sql("INSERT INTO hmpps_court_case_service.offence (id, title, plea) VALUES (:id, :title, CAST(:plea AS jsonb))")
    .dataSource(targetDataSource)
    .build()

  @Bean
  fun offenceStep(sourceJdbcTemplate: JdbcTemplate): Step =
    StepBuilder("offenceStep", jobRepository)
      .chunk<OffenceQueryResult, uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Offence>(batchProperties.chunkSize, transactionManager)
      .reader(reader())
      .processor(processor(sourceJdbcTemplate))
      .writer(writer())
      .build()

  @Bean
  fun job(sourceJdbcTemplate: JdbcTemplate, offenceJobListener: OffenceJobListener): Job =
    JobBuilder("offenceJob", jobRepository)
      .incrementer(RunIdIncrementer())
      .listener(offenceJobListener)
      .start(offenceStep(sourceJdbcTemplate))
      .build()

}


