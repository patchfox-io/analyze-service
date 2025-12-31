package io.patchfox.analyze_service.repositories;

import java.util.List;
import java.util.Set;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import io.patchfox.db_entities.entities.Package;
import io.patchfox.package_utils.util.CvssSeverity;

public interface PackageRepository extends JpaRepository<Package, Long> {
    List<Package> findAllByPurlIn(List<String> packagePurls);

    List<Package> findAllByPurl(String purl);

    @Query(value = "SELECT tabulate_package_index_data_batched(?1, ?2, ?3)", nativeQuery = true)
    void tabulatePackageIndexDataBatched(String purlArrayStr, Long datasetMetricsId, String delimiter);


    // we need to do this in this way to ensure duplicates are preserved in the resultant list
    @Query(
        value = "SELECT p.id " + 
                "FROM package p " + 
                "JOIN unnest(string_to_array(?1, ',')) WITH ORDINALITY AS purls(value, idx) " + 
                "ON p.purl = purls.value " + 
                "ORDER BY purls.idx",
        nativeQuery = true
    )
    List<Long> getIdsForPurls(String stringEncodedPurlArray);


    @Query(value = "SELECT unnest(filter_purls_with_findings(:purlsString))", nativeQuery = true)
    List<String> filterPurlsWithFindings(@Param("purlsString") String purlsString);


    @Query(value = "SELECT unnest(filter_purls_with_findings(" +
                "  (SELECT string_agg(p.purl, ',') " +
                "   FROM package p " +
                "   WHERE p.id = ANY(string_to_array(:packageIdsString, ',')::bigint[]))" +
                "))", nativeQuery = true)
    List<String> filterPackageIdsWithFindings(@Param("packageIdsString") String packageIdsString);


    @Query(value = """
        SELECT p.purl 
        FROM unnest(string_to_array(?1, ',')::bigint[]) WITH ORDINALITY AS input(id, ord)
        JOIN package p ON p.id = input.id
        ORDER BY input.ord
        """, nativeQuery = true)
    List<String> findPurlsByIds(String packageIdsString);

}
