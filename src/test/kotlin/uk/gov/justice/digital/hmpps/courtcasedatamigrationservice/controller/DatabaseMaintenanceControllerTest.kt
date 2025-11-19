package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.controller

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mockito.doNothing
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.springframework.http.HttpStatusCode
import org.springframework.http.ResponseEntity
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.DatabaseMaintenanceService

@ExtendWith(MockitoExtension::class)
class DatabaseMaintenanceControllerTest {

  private val databaseMaintenanceService: DatabaseMaintenanceService = mock(DatabaseMaintenanceService::class.java)
  private val controller = DatabaseMaintenanceController(databaseMaintenanceService)

  @Test
  fun `truncateTables should call service and return success message`() {
    doNothing().`when`(databaseMaintenanceService).truncateTables()
    `when`(databaseMaintenanceService.countMigratedRecords()).thenReturn(150L)

    val response: ResponseEntity<String> = controller.truncateTables()

    assertEquals(HttpStatusCode.valueOf(200), response.statusCode)
    assertEquals("Truncated tables. Total migrated records count -> 150.", response.body)
    verify(databaseMaintenanceService, times(1)).truncateTables()
    verify(databaseMaintenanceService, times(1)).countMigratedRecords()
  }

  @Test
  fun `countMigratedRecords should return response with migrated records count`() {
    `when`(databaseMaintenanceService.countMigratedRecords()).thenReturn(1000L)

    val response: ResponseEntity<String> = controller.countMigratedRecords()

    assertEquals(HttpStatusCode.valueOf(200), response.statusCode)
    assertEquals("Total migrated records count -> 1000", response.body)
    verify(databaseMaintenanceService, times(1)).countMigratedRecords()
  }

  @Test
  fun `listForeignKeys should return response with foreign keys`() {
    `when`(databaseMaintenanceService.findForeignKeys()).thenReturn("Foreign Key {constraint_name=fk_test}")

    val response: ResponseEntity<String> = controller.listForeignKeys()

    assertEquals(HttpStatusCode.valueOf(200), response.statusCode)
    assertEquals("Foreign keys found in the database.\nForeign Key {constraint_name=fk_test}", response.body)
    verify(databaseMaintenanceService, times(1)).findForeignKeys()
  }

  @Test
  fun `dropForeignKeys should call service and return success message`() {
    doNothing().`when`(databaseMaintenanceService).dropForeignKeys()
    `when`(databaseMaintenanceService.findForeignKeys()).thenReturn("Foreign Key {constraint_name=fk_test}")

    val response: ResponseEntity<String> = controller.dropForeignKeys()

    assertEquals(HttpStatusCode.valueOf(200), response.statusCode)
    assertEquals("Foreign keys dropped successfully.\nForeign Key {constraint_name=fk_test}", response.body)
    verify(databaseMaintenanceService, times(1)).dropForeignKeys()
    verify(databaseMaintenanceService, times(1)).findForeignKeys()
  }

  @Test
  fun `recreateForeignKeys should call service and return success message`() {
    doNothing().`when`(databaseMaintenanceService).recreateForeignKeys()
    `when`(databaseMaintenanceService.findForeignKeys()).thenReturn("Foreign Key {constraint_name=fk_test}")

    val response: ResponseEntity<String> = controller.recreateForeignKeys()

    assertEquals(HttpStatusCode.valueOf(200), response.statusCode)
    assertEquals("Foreign keys recreated successfully.\nForeign Key {constraint_name=fk_test}", response.body)
    verify(databaseMaintenanceService, times(1)).recreateForeignKeys()
    verify(databaseMaintenanceService, times(1)).findForeignKeys()
  }
}
