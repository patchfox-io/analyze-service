package io.patchfox.analyze_service.repositories;

import java.time.ZonedDateTime;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import io.patchfox.db_entities.entities.DatasourceMetrics;

public interface DatasourceMetricsRepository extends JpaRepository<DatasourceMetrics, Long> {

    Optional<DatasourceMetrics> findFirstByPurlOrderByCommitDateTimeDesc(String purl);

    long deleteByCommitDateTimeOrCommitDateTimeAfter(ZonedDateTime eventDateTime, ZonedDateTime eventDateTimeAfter);


}
