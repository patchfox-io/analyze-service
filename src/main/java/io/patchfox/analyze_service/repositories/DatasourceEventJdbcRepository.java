package io.patchfox.analyze_service.repositories;

import io.patchfox.db_entities.entities.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Set;
import java.util.UUID;
import java.util.HashSet;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;

@Repository
public class DatasourceEventJdbcRepository {
    
    private final JdbcTemplate jdbcTemplate;
    
    public DatasourceEventJdbcRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
    public DatasourceEvent findByIdFullyHydrated(Long datasourceEventId) {
        // 1. Fetch the DatasourceEvent
        DatasourceEvent event = fetchDatasourceEvent(datasourceEventId);
        
        // 2. Fetch the Datasource (without lazy collections)
        Datasource datasource = fetchDatasource(event.getDatasource().getId());
        event.setDatasource(datasource);
        
        // 3. Fetch all Packages for this event
        Set<io.patchfox.db_entities.entities.Package> packages = fetchPackagesForEvent(datasourceEventId);
        
        if (!packages.isEmpty()) {
            // 4. Fetch all Package IDs
            Set<Long> packageIds = packages.stream()
                .map(io.patchfox.db_entities.entities.Package::getId)
                .collect(Collectors.toSet());
            
            // 5. Fetch all Findings for all packages in bulk
            Map<Long, Set<Finding>> findingsByPackageId = fetchFindingsForPackages(packageIds, "package_finding");
            Map<Long, Set<Finding>> criticalFindingsByPackageId = fetchFindingsForPackages(packageIds, "package_critical_finding");
            Map<Long, Set<Finding>> highFindingsByPackageId = fetchFindingsForPackages(packageIds, "package_high_finding");
            Map<Long, Set<Finding>> mediumFindingsByPackageId = fetchFindingsForPackages(packageIds, "package_medium_finding");
            Map<Long, Set<Finding>> lowFindingsByPackageId = fetchFindingsForPackages(packageIds, "package_low_finding");
            
            // 6. Get all unique finding IDs
            Set<Long> allFindingIds = new HashSet<>();
            allFindingIds.addAll(extractFindingIds(findingsByPackageId));
            allFindingIds.addAll(extractFindingIds(criticalFindingsByPackageId));
            allFindingIds.addAll(extractFindingIds(highFindingsByPackageId));
            allFindingIds.addAll(extractFindingIds(mediumFindingsByPackageId));
            allFindingIds.addAll(extractFindingIds(lowFindingsByPackageId));
            
            // 7. Fetch FindingData and FindingReporters for all findings
            Map<Long, FindingData> findingDataMap = fetchFindingData(allFindingIds);
            Map<Long, Set<FindingReporter>> reportersByFindingId = fetchFindingReporters(allFindingIds);
            
            // 8. Wire up FindingData and Reporters to Findings
            wireUpFindingDetails(findingsByPackageId, findingDataMap, reportersByFindingId);
            wireUpFindingDetails(criticalFindingsByPackageId, findingDataMap, reportersByFindingId);
            wireUpFindingDetails(highFindingsByPackageId, findingDataMap, reportersByFindingId);
            wireUpFindingDetails(mediumFindingsByPackageId, findingDataMap, reportersByFindingId);
            wireUpFindingDetails(lowFindingsByPackageId, findingDataMap, reportersByFindingId);
            
            // 9. Wire up the findings to each package
            for (io.patchfox.db_entities.entities.Package pkg : packages) {
                pkg.setFindings(findingsByPackageId.getOrDefault(pkg.getId(), new HashSet<>()));
                pkg.setCriticalFindings(criticalFindingsByPackageId.getOrDefault(pkg.getId(), new HashSet<>()));
                pkg.setHighFindings(highFindingsByPackageId.getOrDefault(pkg.getId(), new HashSet<>()));
                pkg.setMediumFindings(mediumFindingsByPackageId.getOrDefault(pkg.getId(), new HashSet<>()));
                pkg.setLowFindings(lowFindingsByPackageId.getOrDefault(pkg.getId(), new HashSet<>()));
                pkg.setDatasourceEvents(new HashSet<>()); // Don't populate circular reference
            }
        }
        
        // 10. Wire up packages to event
        event.setPackages(packages);
        
        return event;
    }
    
    private DatasourceEvent fetchDatasourceEvent(Long id) {
        String sql = """
            SELECT id, purl, txid, job_id, commit_hash, commit_branch, 
                commit_date_time, event_date_time, payload, status, 
                processing_error, oss_enriched, package_index_enriched, 
                analyzed, forecasted, recommended, datasource_id
            FROM datasource_event
            WHERE id = ?
            """;
        
        return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
            DatasourceEvent event = new DatasourceEvent();
            event.setId(rs.getLong("id"));
            event.setPurl(rs.getString("purl"));
            event.setTxid(UUID.fromString(rs.getString("txid")));
            
            String jobIdStr = rs.getString("job_id");
            event.setJobId(jobIdStr != null ? UUID.fromString(jobIdStr) : null);
            
            event.setCommitHash(rs.getString("commit_hash"));
            event.setCommitBranch(rs.getString("commit_branch"));
            event.setCommitDateTime(getZonedDateTime(rs, "commit_date_time"));
            event.setEventDateTime(getZonedDateTime(rs, "event_date_time"));
            
            // Fetch and set the payload using reflection
            byte[] payload = rs.getBytes("payload");
            
            try {
                java.lang.reflect.Field payloadField = DatasourceEvent.class.getDeclaredField("payload");
                payloadField.setAccessible(true);
                payloadField.set(event, payload);
                
                // Verify it was set
                byte[] verifyPayload = (byte[]) payloadField.get(event);
                if (verifyPayload == null) {
                    throw new RuntimeException("Payload field is still null after setting for event id " + id);
                }
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException("Failed to set payload field for event id " + id + ": " + e.getMessage(), e);
            }
            
            event.setStatus(DatasourceEvent.Status.valueOf(rs.getString("status")));
            event.setProcessingError(rs.getString("processing_error"));
            event.setOssEnriched(rs.getBoolean("oss_enriched"));
            event.setPackageIndexEnriched(rs.getBoolean("package_index_enriched"));
            event.setAnalyzed(rs.getBoolean("analyzed"));
            event.setForecasted(rs.getBoolean("forecasted"));
            event.setRecommended(rs.getBoolean("recommended"));
            
            // Create a stub Datasource with just the ID for now
            Datasource ds = new Datasource();
            ds.setId(rs.getLong("datasource_id"));
            event.setDatasource(ds);
            
            return event;
        }, id);
    }
    
    private Datasource fetchDatasource(Long id) {
        String sql = """
            SELECT id, latest_txid, latest_job_id, purl, domain, name, 
                   commit_branch, type, number_events_received, 
                   number_event_processing_errors, first_event_received_at, 
                   last_event_received_at, last_event_received_status, status
            FROM datasource
            WHERE id = ?
            """;
        
        return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
            Datasource ds = new Datasource();
            ds.setId(rs.getLong("id"));
            
            String latestTxidStr = rs.getString("latest_txid");
            ds.setLatestTxid(latestTxidStr != null ? UUID.fromString(latestTxidStr) : null);
            
            String latestJobIdStr = rs.getString("latest_job_id");
            ds.setLatestJobId(latestJobIdStr != null ? UUID.fromString(latestJobIdStr) : null);
            
            ds.setPurl(rs.getString("purl"));
            ds.setDomain(rs.getString("domain"));
            ds.setName(rs.getString("name"));
            ds.setCommitBranch(rs.getString("commit_branch"));
            ds.setType(rs.getString("type"));
            ds.setNumberEventsReceived(rs.getDouble("number_events_received"));
            ds.setNumberEventProcessingErrors(rs.getDouble("number_event_processing_errors"));
            ds.setFirstEventReceivedAt(getZonedDateTime(rs, "first_event_received_at"));
            ds.setLastEventReceivedAt(getZonedDateTime(rs, "last_event_received_at"));
            ds.setLastEventReceivedStatus(rs.getString("last_event_received_status"));
            ds.setStatus(Datasource.Status.valueOf(rs.getString("status")));
            
            // Don't populate lazy collections (edits, datasets)
            ds.setEdits(new HashSet<>());
            ds.setDatasets(new HashSet<>());
            
            return ds;
        }, id);
    }
    
    private Set<io.patchfox.db_entities.entities.Package> fetchPackagesForEvent(Long eventId) {
        String sql = """
            SELECT p.id, p.purl, p.type, p.namespace, p.name, p.version,
                   p.number_versions_behind_head, p.number_major_versions_behind_head,
                   p.number_minor_versions_behind_head, p.number_patch_versions_behind_head,
                   p.most_recent_version, p.most_recent_version_published_at,
                   p.this_version_published_at, p.updated_at
            FROM package p
            JOIN datasource_event_package dep ON p.id = dep.package_id
            WHERE dep.datasource_event_id = ?
            """;
        
        List<io.patchfox.db_entities.entities.Package> packages = jdbcTemplate.query(sql, (rs, rowNum) -> {
            io.patchfox.db_entities.entities.Package pkg = new io.patchfox.db_entities.entities.Package();
            pkg.setId(rs.getLong("id"));
            pkg.setPurl(rs.getString("purl"));
            pkg.setType(rs.getString("type"));
            pkg.setNamespace(rs.getString("namespace"));
            pkg.setName(rs.getString("name"));
            pkg.setVersion(rs.getString("version"));
            pkg.setNumberVersionsBehindHead(rs.getInt("number_versions_behind_head"));
            pkg.setNumberMajorVersionsBehindHead(rs.getInt("number_major_versions_behind_head"));
            pkg.setNumberMinorVersionsBehindHead(rs.getInt("number_minor_versions_behind_head"));
            pkg.setNumberPatchVersionsBehindHead(rs.getInt("number_patch_versions_behind_head"));
            pkg.setMostRecentVersion(rs.getString("most_recent_version"));
            pkg.setMostRecentVersionPublishedAt(getZonedDateTime(rs, "most_recent_version_published_at"));
            pkg.setThisVersionPublishedAt(getZonedDateTime(rs, "this_version_published_at"));
            pkg.setUpdatedAt(getZonedDateTime(rs, "updated_at"));
            return pkg;
        }, eventId);
        
        return new HashSet<>(packages);
    }
    
    private Map<Long, Set<Finding>> fetchFindingsForPackages(Set<Long> packageIds, String joinTableName) {
        if (packageIds.isEmpty()) return new HashMap<>();
        
        String placeholders = packageIds.stream().map(id -> "?").collect(Collectors.joining(","));
        String sql = String.format("""
            SELECT pf.package_id, f.id, f.identifier
            FROM finding f
            JOIN %s pf ON f.id = pf.finding_id
            WHERE pf.package_id IN (%s)
            """, joinTableName, placeholders);
        
        Map<Long, Set<Finding>> result = new HashMap<>();
        
        jdbcTemplate.query(sql, rs -> {
            Long packageId = rs.getLong("package_id");
            Finding finding = new Finding();
            finding.setId(rs.getLong("id"));
            finding.setIdentifier(rs.getString("identifier"));
            finding.setPackages(new HashSet<>()); // Don't populate circular reference
            result.computeIfAbsent(packageId, k -> new HashSet<>()).add(finding);
        }, packageIds.toArray());
        
        return result;
    }
    
    private Set<Long> extractFindingIds(Map<Long, Set<Finding>> findingsByPackageId) {
        return findingsByPackageId.values().stream()
            .flatMap(Set::stream)
            .map(Finding::getId)
            .collect(Collectors.toSet());
    }
    
    private Map<Long, FindingData> fetchFindingData(Set<Long> findingIds) {
        if (findingIds.isEmpty()) return new HashMap<>();
        
        String placeholders = findingIds.stream().map(id -> "?").collect(Collectors.joining(","));
        String sql = String.format("""
            SELECT id, finding_id, identifier, severity, description, 
                   cpes, reported_at, published_at, patched_in
            FROM finding_data
            WHERE finding_id IN (%s)
            """, placeholders);
        
        Map<Long, FindingData> result = new HashMap<>();
        
        jdbcTemplate.query(sql, rs -> {
            Long findingId = rs.getLong("finding_id");
            FindingData data = new FindingData();
            data.setId(rs.getLong("id"));
            data.setIdentifier(rs.getString("identifier"));
            data.setSeverity(rs.getString("severity"));
            data.setDescription(rs.getString("description"));
            
            // Handle array columns (PostgreSQL specific - adjust if using different DB)
            data.setCpes(parseStringArray(rs.getArray("cpes")));
            data.setPatchedIn(parseStringArray(rs.getArray("patched_in")));
            
            data.setReportedAt(getZonedDateTime(rs, "reported_at"));
            data.setPublishedAt(getZonedDateTime(rs, "published_at"));
            
            result.put(findingId, data);
        }, findingIds.toArray());
        
        return result;
    }
    
    private Map<Long, Set<FindingReporter>> fetchFindingReporters(Set<Long> findingIds) {
        if (findingIds.isEmpty()) return new HashMap<>();
        
        String placeholders = findingIds.stream().map(id -> "?").collect(Collectors.joining(","));
        String sql = String.format("""
            SELECT ftr.finding_id, fr.id, fr.name
            FROM finding_reporter fr
            JOIN finding_to_reporter ftr ON fr.id = ftr.reporter_id
            WHERE ftr.finding_id IN (%s)
            """, placeholders);
        
        Map<Long, Set<FindingReporter>> result = new HashMap<>();
        
        jdbcTemplate.query(sql, rs -> {
            Long findingId = rs.getLong("finding_id");
            FindingReporter reporter = new FindingReporter();
            reporter.setId(rs.getLong("id"));
            reporter.setName(rs.getString("name"));
            reporter.setFindings(new HashSet<>()); // Don't populate circular reference
            result.computeIfAbsent(findingId, k -> new HashSet<>()).add(reporter);
        }, findingIds.toArray());
        
        return result;
    }
    
    private void wireUpFindingDetails(
            Map<Long, Set<Finding>> findingsByPackageId, 
            Map<Long, FindingData> findingDataMap,
            Map<Long, Set<FindingReporter>> reportersByFindingId) {
        
        for (Set<Finding> findings : findingsByPackageId.values()) {
            for (Finding finding : findings) {
                FindingData data = findingDataMap.get(finding.getId());
                if (data != null) {
                    finding.setData(data);
                    data.setFinding(finding);
                }
                
                Set<FindingReporter> reporters = reportersByFindingId.get(finding.getId());
                if (reporters != null) {
                    finding.setReporters(reporters);
                } else {
                    finding.setReporters(new HashSet<>());
                }
            }
        }
    }
    
    // Helper method to convert PostgreSQL timestamptz to ZonedDateTime
    private ZonedDateTime getZonedDateTime(ResultSet rs, String columnName) throws SQLException {
        OffsetDateTime offsetDateTime = rs.getObject(columnName, OffsetDateTime.class);
        return offsetDateTime != null ? offsetDateTime.toZonedDateTime() : null;
    }
    
    private Set<String> parseStringArray(java.sql.Array sqlArray) throws SQLException {
        if (sqlArray == null) return new HashSet<>();
        String[] arr = (String[]) sqlArray.getArray();
        return arr != null ? new HashSet<>(Arrays.asList(arr)) : new HashSet<>();
    }

    public void updateDatasourceEvent(DatasourceEvent event) {
        String sql = """
            UPDATE datasource_event
            SET purl = ?,
                txid = ?::uuid,
                job_id = ?::uuid,
                commit_hash = ?,
                commit_branch = ?,
                commit_date_time = ?,
                event_date_time = ?,
                payload = ?,
                status = ?,
                processing_error = ?,
                oss_enriched = ?,
                package_index_enriched = ?,
                analyzed = ?,
                forecasted = ?,
                recommended = ?,
                datasource_id = ?
            WHERE id = ?
            """;
        
        try {
            // Get the payload field using reflection since it's private
            java.lang.reflect.Field payloadField = DatasourceEvent.class.getDeclaredField("payload");
            payloadField.setAccessible(true);
            byte[] payload = (byte[]) payloadField.get(event);
            
            jdbcTemplate.update(sql,
                event.getPurl(),
                event.getTxid().toString(),
                event.getJobId() != null ? event.getJobId().toString() : null,
                event.getCommitHash(),
                event.getCommitBranch(),
                event.getCommitDateTime() != null ? java.sql.Timestamp.from(event.getCommitDateTime().toInstant()) : null,
                event.getEventDateTime() != null ? java.sql.Timestamp.from(event.getEventDateTime().toInstant()) : null,
                payload,
                event.getStatus().toString(),
                event.getProcessingError(),
                event.isOssEnriched(),
                event.isPackageIndexEnriched(),
                event.isAnalyzed(),
                event.isForecasted(),
                event.isRecommended(),
                event.getDatasource().getId(),
                event.getId()
            );
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to read payload field for update", e);
        }
    }

}
