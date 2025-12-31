package io.patchfox.analyze_service.repositories;

import java.util.List;
import java.util.UUID;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import io.patchfox.db_entities.entities.Dataset;

public interface DatasetRepository extends JpaRepository<Dataset, Long> {
    List<Dataset> findAllByLatestTxid(UUID txid);   
    Optional<Dataset> findByName(String name);


    @Query(
        value = "select count(*) from datasource_dataset dsd where dsd.dataset_id = ?1",
        nativeQuery = true
    )
    Integer getDatasourceCount(Long datasetId);


    @Query (
        value = "select ds.purl " + 
                    "from datasource_dataset dsd " + 
                    "inner join datasource ds on ds.id = dsd.datasource_id " + 
                    "where dsd.dataset_id = ?1",
        nativeQuery = true
    )
    List<String> getDatasourcePurls(Long datasetId);
}
