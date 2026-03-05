package com.example.springkafkapoc.persistence.mapper;

import com.example.springkafkapoc.domain.model.Transaction;
import com.example.springkafkapoc.persistence.entity.TransactionEntity;
import org.mapstruct.Mapper;

/**
 * <b>MapStruct Mapper: Transaction</b>
 *
 * <p>
 * Handles mapping for the main Transaction data.
 *
 * <p>
 * Tutorial Tip: MapStruct is much faster than Reflection-based mappers (like
 * BeanUtils)
 * because it generates plain Java code during the Maven build. You can see the
 * generated implementation in {@code target/generated-sources}.
 */
@Mapper(componentModel = "spring")
public interface TransactionMapper {
    TransactionEntity toEntity(Transaction domain);

    Transaction toDomain(TransactionEntity entity);
}
