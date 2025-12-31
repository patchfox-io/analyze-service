package io.patchfox.analyze_service.repositories;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import io.patchfox.db_entities.entities.Finding;
import io.patchfox.package_utils.util.CvssSeverity;
import io.patchfox.package_utils.util.Pair;

public interface FindingRepository extends JpaRepository<Finding, Long> {

    // Raw query returning arrays of objects
    @Query(
        "SELECT fd.severity, f.identifier " +
        "FROM Package p " +
        "JOIN p.findings f " +
        "JOIN f.data fd " +
        "WHERE p.purl = :purl"
    )
    List<Object[]> findSeverityAndIdentifierArraysByPurl(@Param("purl") String purl);
    
    // Default method in the repository to convert to PatchFox Pair
    default List<io.patchfox.package_utils.util.Pair<CvssSeverity, String>> findSeverityAndIdentifierByPackagePurlId(String purl) {
        return findSeverityAndIdentifierArraysByPurl(purl).stream()
                                                          .map(arr -> new io.patchfox.package_utils.util.Pair<>(
                                                            CvssSeverity.valueOf(String.valueOf(arr[0])), 
                                                            String.valueOf(arr[1])
                                                          ))
                                                          .toList();
    }


    // Raw query returning arrays of objects with support for multiple purls
    @Query(
        "SELECT fd.severity, f.identifier, p.purl " +
        "FROM Package p " +
        "JOIN p.findings f " +
        "JOIN f.data fd " +
        "WHERE p.purl IN :purls"
    )
    List<Object[]> findSeverityAndIdentifierArraysByPurls(@Param("purls") List<String> purls);

    // 
    // TODO I know this is fucky to update the purlsWithFindingsCache as a side effect of this method but this saves us a full db trip 
    // given this query was already getting that information 
    //
    //
    //
    default List<Pair<CvssSeverity, String>> findSeverityAndIdentifierByPackagePurlIds(List<String> purls, Set<String> purlsWithFindingsCache) {
        // return findSeverityAndIdentifierArraysByPurls(purls).stream()
        //                                                     .map(arr -> new io.patchfox.package_utils.util.Pair<>(
        //                                                         CvssSeverity.valueOf(String.valueOf(arr[0])), 
        //                                                         String.valueOf(arr[1])
        //                                                     ))
        //                                                     .toList();

        var rv = new ArrayList<Pair<CvssSeverity, String>>();

        for (var e : findSeverityAndIdentifierArraysByPurls(purls)) {
            rv.add(new Pair<>(CvssSeverity.valueOf(String.valueOf(e[0])), String.valueOf(e[1])));
            purlsWithFindingsCache.add(String.valueOf(e[2]));
        }

        return rv;
    }


    @Query(value = "CALL create_findings_performance_indexes()", nativeQuery = true)
    @Transactional()
    @Modifying
    void createFindingsPerformanceIndexesNative();

}
