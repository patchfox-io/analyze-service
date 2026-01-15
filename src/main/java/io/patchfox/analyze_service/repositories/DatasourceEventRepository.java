package io.patchfox.analyze_service.repositories;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import io.patchfox.db_entities.entities.DatasourceEvent;

public interface DatasourceEventRepository extends JpaRepository<DatasourceEvent, Long> {

    // doing this in this manner to force hibernate to load the entire package collection at once instead of n+1 
    @Query("SELECT DISTINCT de FROM DatasourceEvent de " +
        "LEFT JOIN FETCH de.datasource " +
        "LEFT JOIN FETCH de.packages p " +
        "WHERE de.id = :id")
    Optional<DatasourceEvent> findByIdWithPackages(@Param("id") Long id);

    // ok if things blow up if optional is not present. that should never happen
    // TODO add better error handling - ok to leave for now but eventually we don't want one errant call to 
    // make the service go boom 
    default public DatasourceEvent findByIdWithPackagesImpl(long id) { return findByIdWithPackages(id).get(); }

    @Query("SELECT p.purl FROM DatasourceEvent de " +
        "JOIN de.packages p " +
        "WHERE de.id = :id")
    List<String> findPackagePurlsByEventId(@Param("id") Long id);

    List<DatasourceEvent> findAllByTxidAndCommitDateTime(UUID txid, ZonedDateTime commitDateTime);

    List<DatasourceEvent> findAllByTxid(UUID txid);

    List<DatasourceEvent> findAllByCommitDateTime(ZonedDateTime commitDateTime);

    List<DatasourceEvent> findAllByCommitDateTimeAndStatus(
        ZonedDateTime commitDateTime, 
        DatasourceEvent.Status status
    );
    

}
