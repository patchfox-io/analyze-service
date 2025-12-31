package io.patchfox.analyze_service.repositories;

import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.Set;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import io.patchfox.db_entities.entities.Datasource;

public interface DatasourceRepository extends JpaRepository<Datasource, Long> {

    // @Query(value = "SELECT p.purl FROM package p " +
    //                 "WHERE p.id IN (SELECT unnest(d.package_indexes) FROM datasource d WHERE d.purl = :datasourcePurl)", 
    //         nativeQuery = true)
    @Query(value = "SELECT p.purl " +
                "FROM datasource d " +
                // Lateral join that explodes the array into rows (pkg_id)
                "JOIN unnest(d.package_indexes) AS pkg_id ON TRUE " +
                // Join the resulting IDs to the package table
                "JOIN package p ON p.id = pkg_id " +
                "WHERE d.purl = :datasourcePurl", 
                nativeQuery = true)    
    Set<String> findPackagePurlsByDatasourcePurl(@Param("datasourcePurl") String datasourcePurl);

}
