package com.example.springkafkapoc.domain.model;

/**
 * <b>Audit Event Types</b>
 *
 * <p>Shared enum across persistence and domain layers.
 */
public enum AuditEventType {
  RECEIVED,
  PROCESSED,
  RETRY_ATTEMPTED,
  SENT_TO_DLQ,
  ANALYTICS_AGGREGATED,
  BIGQUERY_WRITTEN
}
