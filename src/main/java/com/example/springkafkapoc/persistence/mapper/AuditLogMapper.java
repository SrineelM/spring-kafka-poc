package com.example.springkafkapoc.persistence.mapper;

import com.example.springkafkapoc.domain.model.AuditLog;
import com.example.springkafkapoc.persistence.entity.AuditLogEntity;
import org.mapstruct.Mapper;

/**
 * <b>MapStruct Mapper: AuditLog</b>
 *
 * <p>
 * Automates the conversion between {@link AuditLog} (Domain) and
 * {@link AuditLogEntity} (JPA).
 *
 * <p>
 * WHY MAPSTRUCT? Manual mapping (setters/getters) is error-prone and tedious.
 * MapStruct generates the implementation at compile-time, ensuring high
 * performance
 * and fail-fast behavior if fields are missing or mismatched.
 */
@Mapper(componentModel = "spring")
public interface AuditLogMapper {
    AuditLogEntity toEntity(AuditLog domain);

    AuditLog toDomain(AuditLogEntity entity);
}
