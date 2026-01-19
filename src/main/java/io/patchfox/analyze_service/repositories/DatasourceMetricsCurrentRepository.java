package io.patchfox.analyze_service.repositories;

import java.time.ZonedDateTime;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import io.patchfox.db_entities.entities.DatasourceMetricsCurrent;
import jakarta.transaction.Transactional;

public interface DatasourceMetricsCurrentRepository extends JpaRepository<DatasourceMetricsCurrent, Long> {

    @Modifying
    @Transactional
    @Query("DELETE FROM DatasourceMetricsCurrent dmc WHERE dmc.commitDateTime = :eventDateTime OR dmc.commitDateTime >= :eventDateTimeAfter")
    int deleteByCommitDateTimeOrCommitDateTimeAfter(
        @Param("eventDateTime") ZonedDateTime eventDateTime,
        @Param("eventDateTimeAfter") ZonedDateTime eventDateTimeAfter
    );


}
