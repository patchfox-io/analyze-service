package io.patchfox.analyze_service.helpers;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.hibernate.Hibernate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import io.patchfox.analyze_service.repositories.DatasetMetricsRepository;
import io.patchfox.analyze_service.repositories.DatasetRepository;
import io.patchfox.analyze_service.repositories.DatasourceEventRepository;
import io.patchfox.analyze_service.repositories.DatasourceMetricsCurrentRepository;
import io.patchfox.analyze_service.repositories.DatasourceMetricsRepository;
import io.patchfox.analyze_service.repositories.DatasourceRepository;
import io.patchfox.analyze_service.repositories.EditRepository;
import io.patchfox.analyze_service.repositories.FindingRepository;
import io.patchfox.analyze_service.repositories.PackageRepository;
import io.patchfox.db_entities.entities.Dataset;
import io.patchfox.db_entities.entities.DatasetMetrics;
import io.patchfox.db_entities.entities.DatasourceEvent;
import io.patchfox.db_entities.entities.Edit;
import io.patchfox.package_utils.util.CvssSeverity;
import io.patchfox.package_utils.util.Pair;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Transactional
@Component
public class TabulateServiceTransactionalHelpers {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    DatasetRepository datasetRepository;

    @Autowired
    DatasourceEventRepository datasourceEventRepository; 

    @Autowired
    DatasetMetricsRepository datasetMetricsRepository;

    @Autowired
    DatasourceMetricsRepository datasourceMetricsRepository;

    @Autowired
    DatasourceMetricsCurrentRepository datasourceMetricsCurrentRepository;

    @Autowired
    PackageRepository packageRepository;

    @Autowired
    EditRepository editRepository;

    @Autowired 
    FindingRepository findingRepository;

    @Autowired
    DatasourceRepository datasourceRepository;


    /**
     * 
     * @param id
     * @return
     */
    public DatasourceEvent getDatasourceEventWithPackagesForId(long id) { 
        var dse = datasourceEventRepository.findById(id).get();
        // the packages collection in the dse object are lazily instantiated. 
        // we want the obj to have the packages resolved and we need to do that from within
        // an @Transactional context .
        Hibernate.initialize(dse.getPackages());
        return dse;
    }




    public void deleteMetricsByCommitDateTimeOrCommitDateTimeAfter(ZonedDateTime firstEventCommitDatetime) {
        datasetMetricsRepository.deleteByCommitDateTimeOrCommitDateTimeAfter(
            firstEventCommitDatetime, 
            firstEventCommitDatetime
        );

        datasourceMetricsRepository.deleteByCommitDateTimeOrCommitDateTimeAfter(
            firstEventCommitDatetime, 
            firstEventCommitDatetime
        );

        datasourceMetricsCurrentRepository.deleteByCommitDateTimeOrCommitDateTimeAfter(
            firstEventCommitDatetime, 
            firstEventCommitDatetime
        );
    }



    /**
     * 
     * @param timestamp
     * @return
     */
    public List<DatasetMetrics> getHistoricalDatasetMetricRecordsByEventDateAsc(ZonedDateTime timestamp) {
        var datasetMetrics = 
            datasetMetricsRepository.findAllByIsCurrentAndCommitDateTimeAfterOrderByCommitDateTimeAsc(true, timestamp);
        for (var dsm : datasetMetrics) {
            // the packages collection in the dse object are lazily instantiated. 
            // we want the obj to have the packages resolved and we need to do that from within
            // an @Transactional context .
            Hibernate.initialize(dsm.getEdits());
        }

        return datasetMetrics;
    }


    /**
     * 
     * @param name
     * @return
     */
    public Dataset getDatasetRecordForName(String name) {
        return datasetRepository.findByName(name).get();
    }


    /**
     * 
     * @param id
     * @return
     */
    public DatasourceEvent getDatasourceEventRecordForId(long id) {
        return datasourceEventRepository.findById(id).get();
    }


    /**
     * 
     * @param purls
     * @return
     */
    public List<io.patchfox.db_entities.entities.Package> getPackagesByPurlIn(List<String> purls) {
        return packageRepository.findAllByPurlIn(purls);
    }


    /**
     * 
     * @param commitDateTime
     * @param purls
     * @return
     */
    public List<Edit> getAllByCommitDateTimeAndDatasourcePurlOnOrBeforeAsc(ZonedDateTime commitDateTime, List<String> purls) {
        return editRepository.findAllByCommitDateTimeAndDatasourcePurlInOrCommitDateTimeBeforeAndDatasourcePurlInOrderByCommitDateTimeAsc(
            commitDateTime,
            purls,
            commitDateTime,
            purls
        );
    }


    public List<Pair<CvssSeverity, String>> findSeverityAndIdentifierByPackagePurlIdsTransactional(List<String> purls, Set<String> purlsWithFindingsCache) {
        return findingRepository.findSeverityAndIdentifierByPackagePurlIds(purls, purlsWithFindingsCache);
    }


    public Set<String> findPackagePurlsByDatasourcePurlTransactional(String datasourcePurl) { 
        return datasourceRepository.findPackagePurlsByDatasourcePurl(datasourcePurl);
    }


    public List<DatasetMetrics> findAllByIsCurrentAndCommitDateTimeBeforeOrderByCommitDateTimeDesc(
        boolean isCurrent,
        ZonedDateTime commitDateTime,
        int limit
    ) {
        String sql = """
            SELECT 
                dm.*,
                d.id as dataset_id,
                d.name as dataset_name,
                d.latest_txid as dataset_latest_txid,
                d.latest_job_id as dataset_latest_job_id,
                d.updated_at as dataset_updated_at,
                d.status as dataset_status
            FROM dataset_metrics dm
            INNER JOIN dataset d ON dm.dataset_id = d.id
            WHERE dm.is_current = ? 
            AND dm.commit_date_time < ? 
            ORDER BY dm.commit_date_time DESC 
            LIMIT ?
        """;
        
        List<DatasetMetrics> metrics = jdbcTemplate.query(sql, 
            ps -> {
                ps.setBoolean(1, isCurrent);
                ps.setObject(2, commitDateTime.toOffsetDateTime());
                ps.setInt(3, limit);
            },
            this::mapRowToDatasetMetrics
        );
        
        // Load relationship data for each metric
        for (DatasetMetrics metric : metrics) {
            loadPackageFamilies(metric);
            loadPackageIndexes(metric);
            loadEdits(metric);
        }
        
        return metrics;
    }

    private DatasetMetrics mapRowToDatasetMetrics(ResultSet rs, int rowNum) throws SQLException {
        // Create Dataset entity
        Dataset dataset = new Dataset();
        dataset.setId(rs.getLong("dataset_id"));
        dataset.setName(rs.getString("dataset_name"));
        dataset.setLatestTxid(toUUID(rs.getString("dataset_latest_txid")));
        dataset.setLatestJobId(toUUID(rs.getString("dataset_latest_job_id")));
        dataset.setUpdatedAt(toZonedDateTime(rs.getObject("dataset_updated_at", OffsetDateTime.class)));
        dataset.setStatus(toDatasetStatus(rs.getString("dataset_status")));
        
        // Create DatasetMetrics with all scalar fields
        DatasetMetrics metrics = new DatasetMetrics();
        metrics.setDataset(dataset);
        metrics.setId(rs.getLong("id"));
        metrics.setDatasourceCount(rs.getLong("datasource_count"));
        metrics.setDatasourceEventCount(rs.getLong("datasource_event_count"));
        metrics.setCommitDateTime(toZonedDateTime(rs.getObject("commit_date_time", OffsetDateTime.class)));
        metrics.setEventDateTime(toZonedDateTime(rs.getObject("event_date_time", OffsetDateTime.class)));
        metrics.setForecastMaturityDate(toZonedDateTime(rs.getObject("forecast_maturity_date", OffsetDateTime.class)));
        metrics.setTxid(UUID.fromString(rs.getString("txid")));
        metrics.setJobId(UUID.fromString(rs.getString("job_id")));
        metrics.setCurrent(rs.getBoolean("is_current"));
        metrics.setForecastSameCourse(rs.getBoolean("is_forecast_same_course"));
        metrics.setForecastRecommendationsTaken(rs.getBoolean("is_forecast_recommendations_taken"));
        metrics.setRecommendationType(toRecommendationType(rs.getString("recommendation_type")));
        metrics.setRecommendationHeadline(rs.getString("recommendation_headline"));
        metrics.setRpsScore(rs.getDouble("rps_score"));
        metrics.setTotalFindings(rs.getLong("total_findings"));
        metrics.setCriticalFindings(rs.getLong("critical_findings"));
        metrics.setHighFindings(rs.getLong("high_findings"));
        metrics.setMediumFindings(rs.getLong("medium_findings"));
        metrics.setLowFindings(rs.getLong("low_findings"));
        metrics.setFindingsAvoidedByPatchingPastYear(rs.getLong("findings_avoided_by_patching_past_year"));
        metrics.setCriticalFindingsAvoidedByPatchingPastYear(rs.getLong("critical_findings_avoided_by_patching_past_year"));
        metrics.setHighFindingsAvoidedByPatchingPastYear(rs.getLong("high_findings_avoided_by_patching_past_year"));
        metrics.setMediumFindingsAvoidedByPatchingPastYear(rs.getLong("medium_findings_avoided_by_patching_past_year"));
        metrics.setLowFindingsAvoidedByPatchingPastYear(rs.getLong("low_findings_avoided_by_patching_past_year"));
        metrics.setFindingsInBacklogBetweenThirtyAndSixtyDays(rs.getDouble("findings_in_backlog_between_thirty_and_sixty_days"));
        metrics.setCriticalFindingsInBacklogBetweenThirtyAndSixtyDays(rs.getDouble("critical_findings_in_backlog_between_thirty_and_sixty_days"));
        metrics.setHighFindingsInBacklogBetweenThirtyAndSixtyDays(rs.getDouble("high_findings_in_backlog_between_thirty_and_sixty_days"));
        metrics.setMediumFindingsInBacklogBetweenThirtyAndSixtyDays(rs.getDouble("medium_findings_in_backlog_between_thirty_and_sixty_days"));
        metrics.setLowFindingsInBacklogBetweenThirtyAndSixtyDays(rs.getDouble("low_findings_in_backlog_between_thirty_and_sixty_days"));
        metrics.setFindingsInBacklogBetweenSixtyAndNinetyDays(rs.getDouble("findings_in_backlog_between_sixty_and_ninety_days"));
        metrics.setCriticalFindingsInBacklogBetweenSixtyAndNinetyDays(rs.getDouble("critical_findings_in_backlog_between_sixty_and_ninety_days"));
        metrics.setHighFindingsInBacklogBetweenSixtyAndNinetyDays(rs.getDouble("high_findings_in_backlog_between_sixty_and_ninety_days"));
        metrics.setMediumFindingsInBacklogBetweenSixtyAndNinetyDays(rs.getDouble("medium_findings_in_backlog_between_sixty_and_ninety_days"));
        metrics.setLowFindingsInBacklogBetweenSixtyAndNinetyDays(rs.getDouble("low_findings_in_backlog_between_sixty_and_ninety_days"));
        metrics.setFindingsInBacklogOverNinetyDays(rs.getDouble("findings_in_backlog_over_ninety_days"));
        metrics.setCriticalFindingsInBacklogOverNinetyDays(rs.getDouble("critical_findings_in_backlog_over_ninety_days"));
        metrics.setHighFindingsInBacklogOverNinetyDays(rs.getDouble("high_findings_in_backlog_over_ninety_days"));
        metrics.setMediumFindingsInBacklogOverNinetyDays(rs.getDouble("medium_findings_in_backlog_over_ninety_days"));
        metrics.setLowFindingsInBacklogOverNinetyDays(rs.getDouble("low_findings_in_backlog_over_ninety_days"));
        metrics.setPackages(rs.getLong("packages"));
        metrics.setPackagesWithFindings(rs.getLong("packages_with_findings"));
        metrics.setPackagesWithCriticalFindings(rs.getLong("packages_with_critical_findings"));
        metrics.setPackagesWithHighFindings(rs.getLong("packages_with_high_findings"));
        metrics.setPackagesWithMediumFindings(rs.getLong("packages_with_medium_findings"));
        metrics.setPackagesWithLowFindings(rs.getLong("packages_with_low_findings"));
        metrics.setDownlevelPackages(rs.getLong("downlevel_packages"));
        metrics.setDownlevelPackagesMajor(rs.getLong("downlevel_packages_major"));
        metrics.setDownlevelPackagesMinor(rs.getLong("downlevel_packages_minor"));
        metrics.setDownlevelPackagesPatch(rs.getLong("downlevel_packages_patch"));
        metrics.setStalePackages(rs.getLong("stale_packages"));
        metrics.setStalePackagesSixMonths(rs.getLong("stale_packages_six_months"));
        metrics.setStalePackagesOneYear(rs.getLong("stale_packages_one_year"));
        metrics.setStalePackagesOneYearSixMonths(rs.getLong("stale_packages_one_year_six_months"));
        metrics.setStalePackagesTwoYears(rs.getLong("stale_packages_two_years"));
        metrics.setPatches(rs.getLong("patches"));
        metrics.setSamePatches(rs.getLong("same_patches"));
        metrics.setDifferentPatches(rs.getLong("different_patches"));
        metrics.setPatchFoxPatches(rs.getLong("patch_fox_patches"));
        metrics.setPatchEfficacyScore(rs.getDouble("patch_efficacy_score"));
        metrics.setPatchImpact(rs.getDouble("patch_impact"));
        metrics.setPatchEffort(rs.getDouble("patch_effort"));
        
        return metrics;
    }

    private UUID toUUID(String value) {
        return value != null ? UUID.fromString(value) : null;
    }

    private Dataset.Status toDatasetStatus(String value) {
        return value != null ? Dataset.Status.valueOf(value) : null;
    }

    private void loadPackageFamilies(DatasetMetrics metrics) {
        String sql = "SELECT package_family FROM package_family WHERE dataset_metrics_id = ?";
        Set<String> families = new HashSet<>(
            jdbcTemplate.queryForList(sql, String.class, metrics.getId())
        );
        metrics.setPackageFamilies(families);
    }

    private void loadPackageIndexes(DatasetMetrics metrics) {
        // PostgreSQL stores arrays differently - need to handle the array type
        String sql = "SELECT package_indexes FROM dataset_metrics WHERE id = ?";
        Array array = jdbcTemplate.queryForObject(sql, 
            (rs, rowNum) -> rs.getArray("package_indexes"), 
            metrics.getId()
        );
        
        if (array != null) {
            try {
                Long[] indexes = (Long[]) array.getArray();
                metrics.setPackageIndexes(Arrays.asList(indexes));
            } catch (SQLException e) {
                throw new RuntimeException("Failed to load package indexes", e);
            }
        }
    }

    private void loadEdits(DatasetMetrics metrics) {
        String sql = "SELECT * FROM edit WHERE dataset_metrics_id = ?";
        Set<Edit> edits = new HashSet<>(
            jdbcTemplate.query(sql, this::mapRowToEdit, metrics.getId())
        );
        metrics.setEdits(edits);
    }

    private Edit mapRowToEdit(ResultSet rs, int rowNum) throws SQLException {
        // Map Edit entity - adjust based on your Edit table structure
        Edit edit = new Edit();
        edit.setId(rs.getLong("id"));
        // ... add other Edit fields as needed
        return edit;
    }

    private ZonedDateTime toZonedDateTime(OffsetDateTime offsetDateTime) {
        return offsetDateTime != null ? offsetDateTime.toZonedDateTime() : null;
    }

    private DatasetMetrics.RecommendationType toRecommendationType(String value) {
        return value != null ? DatasetMetrics.RecommendationType.valueOf(value) : null;
    }

}