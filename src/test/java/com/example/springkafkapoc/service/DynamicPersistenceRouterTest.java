package com.example.springkafkapoc.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.springkafkapoc.domain.model.Transaction;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * <b>Persistence Routing Unit Test</b>
 *
 * <p><b>TUTORIAL: TESTING THE STRATEGY PATTERN</b>
 *
 * <ul>
 *   <li><b>Objective:</b> We want to verify that the {@link DynamicPersistenceRouter} correctly
 *       selects the right storage engine based on what's available.
 *   <li><b>Mocking:</b> We use {@code @Mock} to create "Stubs" for the real Spanner and H2
 *       services. This ensures we are testing the ONLY the routing logic, not the actual database
 *       connections.
 *   <li><b>Verification:</b> {@code verify(spannerService).save(...)} is how we confirm that the
 *       router actually called the specific instance we expected.
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
class DynamicPersistenceRouterTest {

  @Mock private TransactionPersistencePort spannerService;

  @Mock private TransactionPersistencePort h2Service;

  @Test
  void shouldRouteToSpannerWhenPresent() {
    // Given: Both Spanner and H2 are registered, but Spanner has priority
    when(spannerService.getStoreName()).thenReturn("GCP Spanner");
    when(h2Service.getStoreName()).thenReturn("H2 In-Memory");

    // The router receives a list of all available providers
    DynamicPersistenceRouter router =
        new DynamicPersistenceRouter(List.of(spannerService, h2Service));

    Transaction transaction = Transaction.builder().transactionId("transaction-1").build();
    when(spannerService.save(any(Transaction.class))).thenReturn(transaction);

    // When: We try to save a transaction
    router.save(transaction);

    // Then: The router should have picked Spanner
    verify(spannerService).save(transaction);
  }

  @Test
  void shouldRouteToH2WhenSpannerAbsent() {
    // Given: ONLY the H2 provider is available in the list
    when(h2Service.getStoreName()).thenReturn("H2 In-Memory");

    DynamicPersistenceRouter router = new DynamicPersistenceRouter(List.of(h2Service));

    Transaction transaction = Transaction.builder().transactionId("transaction-1").build();
    when(h2Service.save(any(Transaction.class))).thenReturn(transaction);

    // When: We try to save a transaction
    router.save(transaction);

    // Then: The router should have fallen back to H2
    verify(h2Service).save(transaction);
  }
}
