package io.patchfox.analyze_service.repositories;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import io.patchfox.db_entities.entities.DatasetMetrics;
import io.patchfox.package_utils.util.Pair;
import jakarta.transaction.Transactional;

public interface DatasetMetricsRepository extends JpaRepository<DatasetMetrics, Long> {

    @Modifying
    @Transactional
    @Query("DELETE FROM DatasetMetrics dm WHERE dm.commitDateTime = :eventDateTime OR dm.commitDateTime >= :eventDateTimeAfter")
    int deleteByCommitDateTimeOrCommitDateTimeAfter(
        @Param("eventDateTime") ZonedDateTime eventDateTime,
        @Param("eventDateTimeAfter") ZonedDateTime eventDateTimeAfter
    );

    List<DatasetMetrics> findAllByIsCurrentAndCommitDateTimeAfterOrderByCommitDateTimeAsc(
        boolean isCurrent,
        ZonedDateTime eventDateTime
    );



    @Query("SELECT dsm.id, dsm.commitDateTime " +
       "FROM DatasetMetrics dsm " +
       "WHERE dsm.isCurrent = true " +
       "AND dsm.commitDateTime < :commitDateTime " +
       "ORDER BY dsm.commitDateTime ASC")
    List<Object[]> findIdsAndCommitDateTimesByCommitDateTimeBeforeImpl(
        @Param("commitDateTime") ZonedDateTime commitDateTime
    );


    default List<Pair<Long, ZonedDateTime>> findIdsAndCommitDateTimesByCommitDateTimeBefore(ZonedDateTime commitDateTime) {
        var rvRaw = findIdsAndCommitDateTimesByCommitDateTimeBeforeImpl(commitDateTime);
        return rvRaw.stream().map(x -> new Pair<>((Long)x[0], (ZonedDateTime)x[1])).collect(Collectors.toList());
    }


    @Query(value = """
            SELECT * FROM dataset_metrics 
            WHERE is_current = :isCurrent 
            AND commit_date_time < :commitDateTime 
            ORDER BY commit_date_time ASC 
            LIMIT :limit
        """, nativeQuery = true)
    List<DatasetMetrics> findAllByIsCurrentAndCommitDateTimeBeforeOrderByCommitDateTimeAsc(
        @Param("isCurrent") boolean isCurrent,
        @Param("commitDateTime") ZonedDateTime commitDateTime,
        @Param("limit") int limit
    );


    @Query(value = """
            SELECT * FROM dataset_metrics 
            WHERE is_current = :isCurrent 
            AND commit_date_time < :commitDateTime 
            ORDER BY commit_date_time DESC 
            LIMIT :limit
        """, nativeQuery = true)
    List<DatasetMetrics> findAllByIsCurrentAndCommitDateTimeBeforeOrderByCommitDateTimeDesc(
        @Param("isCurrent") boolean isCurrent,
        @Param("commitDateTime") ZonedDateTime commitDateTime,
        @Param("limit") int limit
    );


    @Modifying
    @Transactional
    @Query(value = "CALL update_dataset_metrics_findings_counts(?1, ?2)", nativeQuery = true)
    void updateDatasetMetricsFindings(String purlsString, Long datasetMetricsId);


    //@Modifying
    //@Transactional
    @Query(value = "SELECT update_edit_and_dataset_metrics_findings(?1, ?2, ?3, ?4, ?5)", nativeQuery = true)
    void updateEditAndDatasetMetricsFindings(
        String editPurlsString, 
        ZonedDateTime commitDateTime, 
        String datasourcePurl, 
        String purlsString, 
        Long datasetMetricsId
    );
    

    // @Modifying
    // @Transactional
    // @Query(value = "CALL filter_and_update_findings_mega(?1, ?2, ?3, ?4, ?5, ?6)", nativeQuery = true)
    // void filterAndUpdateFindingsMega(
    //     String filterPackagePurlsList,
    //     String editPackagePurlsList, 
    //     ZonedDateTime editCommitDateTime,
    //     String editDatasourcePurl,
    //     String datasetPackagePurlsList,
    //     Long datasetMetricsId
    // );


}
