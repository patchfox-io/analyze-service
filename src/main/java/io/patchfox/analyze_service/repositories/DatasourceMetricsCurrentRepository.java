package io.patchfox.analyze_service.repositories;

import java.time.ZonedDateTime;

import org.springframework.data.jpa.repository.JpaRepository;
import io.patchfox.db_entities.entities.DatasourceMetricsCurrent;

public interface DatasourceMetricsCurrentRepository extends JpaRepository<DatasourceMetricsCurrent, Long> {

    long deleteByCommitDateTimeOrCommitDateTimeAfter(ZonedDateTime eventDateTime, ZonedDateTime eventDateTimeAfter);


}
