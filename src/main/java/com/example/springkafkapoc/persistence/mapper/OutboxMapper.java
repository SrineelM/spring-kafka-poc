package com.example.springkafkapoc.persistence.mapper;

import com.example.springkafkapoc.domain.model.Outbox;
import com.example.springkafkapoc.persistence.entity.OutboxEntity;
import org.mapstruct.Mapper;

/**
 * <b>Outbox Mapper</b>
 *
 * <p>
 * <b>TUTORIAL:</b> This mapper bridges the gap between our <b>Persistence
 * Entity</b> (used by JPA/Hibernate)
 * and our <b>Domain Model</b> (used by business services).
 *
 * <p>
 * <b>Pattern:</b> Decoupling the Domain from Persistence prevents database
 * changes from leaking
 * into your message-processing logic.
 */
@Mapper(componentModel = "spring")
public interface OutboxMapper {
    OutboxEntity toEntity(Outbox domain);

    Outbox toDomain(OutboxEntity entity);
}
