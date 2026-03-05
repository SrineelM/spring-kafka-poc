package com.example.springkafkapoc.controller;

import com.example.springkafkapoc.service.AnalyticsQueryService;
import java.math.BigDecimal;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * <b>Real-time Analytics API</b>
 *
 * <p>
 * Exposes outcomes from the Kafka Streams processing topology.
 *
 * <p>
 * Tutorial Tip: This controller demonstrates <b>Interactive Queries</b>.
 * Instead of querying a traditional database, it queries a <b>State Store</b>
 * that resides directly within the Kafka Streams application memory/disk.
 * This provides ultra-low latency access to real-time aggregates.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/analytics")
@RequiredArgsConstructor
public class AnalyticsController {

    private final AnalyticsQueryService analyticsQueryService;

    /**
     * Gets the 24-hour spending total for an account.
     *
     * @param accountId unique account ID
     * @return 24h rolling total
     */
    @GetMapping("/daily-total/{accountId}")
    public ResponseEntity<BigDecimal> getDailyTotal(@PathVariable String accountId) {
        log.info("Querying daily total for account: {}", accountId);
        BigDecimal total = analyticsQueryService.getDailyAccountTotal(accountId);
        return ResponseEntity.ok(total != null ? total : BigDecimal.ZERO);
    }
}
