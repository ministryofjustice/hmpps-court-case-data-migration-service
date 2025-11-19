package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.doNothing
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.util.SqlFileExecutor

@ExtendWith(MockitoExtension::class)
class DatabaseMaintenanceServiceTest {

  private val sqlFileExecutor: SqlFileExecutor = mock(SqlFileExecutor::class.java)
  private val service = DatabaseMaintenanceService(sqlFileExecutor)

  @Test
  fun `truncateTables should call executeVoid with correct file`() {
    doNothing().`when`(sqlFileExecutor).executeVoid(anyString())

    service.truncateTables()

    verify(sqlFileExecutor, times(1)).executeVoid("sql/truncate_tables.sql")
  }

  @Test
  fun `countMigratedRecords should call executeScalar with expected records count`() {
    `when`(sqlFileExecutor.executeScalar("sql/count_migrated_records.sql", Long::class.java)).thenReturn(2000L)

    val result = service.countMigratedRecords()

    assertEquals(2000L, result)
    verify(sqlFileExecutor, times(1)).executeScalar("sql/count_migrated_records.sql", Long::class.java)
  }

  @Test
  fun `dropForeignKeys should call executeVoid with correct file`() {
    doNothing().`when`(sqlFileExecutor).executeVoid(anyString())

    service.dropForeignKeys()

    verify(sqlFileExecutor, times(1)).executeVoid("sql/drop_foreign_keys.sql")
  }

  @Test
  fun `recreateForeignKeys should call executeVoid with correct file`() {
    doNothing().`when`(sqlFileExecutor).executeVoid(anyString())

    service.recreateForeignKeys()

    verify(sqlFileExecutor, times(1)).executeVoid("sql/recreate_foreign_keys.sql")
  }

  @Test
  fun `findForeignKeys should return formatted string when foreign keys exist`() {
    val mockResult = listOf(
      mapOf("constraint_name" to "fk_test", "table_name" to "test_table"),
    )
    `when`(sqlFileExecutor.executeQuery(anyString())).thenReturn(mockResult)

    val result = service.findForeignKeys()

    assertEquals("Foreign Key {constraint_name=fk_test, table_name=test_table}.", result)
    verify(sqlFileExecutor, times(1)).executeQuery("sql/find_foreign_keys.sql")
  }

  @Test
  fun `findForeignKeys should return default message when no foreign keys found`() {
    `when`(sqlFileExecutor.executeQuery(anyString())).thenReturn(emptyList())

    val result = service.findForeignKeys()

    assertEquals("No Foreign keys found.", result)
  }
}
