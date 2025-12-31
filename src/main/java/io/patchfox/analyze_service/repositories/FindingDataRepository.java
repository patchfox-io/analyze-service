package io.patchfox.analyze_service.repositories;

import java.util.List;
import java.util.Set;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import io.patchfox.db_entities.entities.FindingData;

public interface FindingDataRepository extends JpaRepository<FindingData, Long>{

    @Query(
        value = "SELECT fd.* " +
                "FROM package p " +
                "JOIN package_finding pf ON p.id = pf.package_id " +
                "JOIN finding f ON f.id = pf.finding_id " +
                "JOIN finding_data fd ON fd.finding_id = f.id " +
                "WHERE p.purl = :purl",
        nativeQuery = true
    )
    List<FindingData> findFindingDataForPackagePurl(@Param("purl") String purl);


    @Query(
        value = "SELECT fd.* " +
                "FROM package p " +
                "JOIN package_finding pf ON p.id = pf.package_id " +
                "JOIN finding f ON f.id = pf.finding_id " +
                "JOIN finding_data fd ON fd.finding_id = f.id " +
                "WHERE p.purl IN (:purls)",
        nativeQuery = true
    )
    List<FindingData> findFindingDataForPackagePurls(@Param("purls") Set<String> purls);

}
