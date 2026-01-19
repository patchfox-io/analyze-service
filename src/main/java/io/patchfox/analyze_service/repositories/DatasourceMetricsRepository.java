package io.patchfox.analyze_service.repositories;

import java.time.ZonedDateTime;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import io.patchfox.db_entities.entities.DatasourceMetrics;
import jakarta.transaction.Transactional;

public interface DatasourceMetricsRepository extends JpaRepository<DatasourceMetrics, Long> {

    Optional<DatasourceMetrics> findFirstByPurlOrderByCommitDateTimeDesc(String purl);

    @Modifying
    @Transactional
    @Query("DELETE FROM DatasourceMetrics dsm WHERE dsm.commitDateTime = :eventDateTime OR dsm.commitDateTime >= :eventDateTimeAfter")
    int deleteByCommitDateTimeOrCommitDateTimeAfter(
        @Param("eventDateTime") ZonedDateTime eventDateTime,
        @Param("eventDateTimeAfter") ZonedDateTime eventDateTimeAfter
    );


}
