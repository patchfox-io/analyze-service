package io.patchfox.analyze_service.repositories;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import io.patchfox.db_entities.entities.Edit;
import io.patchfox.package_utils.util.Pair;
import jakarta.transaction.Transactional;

public interface EditRepository extends JpaRepository<Edit, Long> {


    @Modifying
    @Transactional
    @Query(value = "CREATE INDEX IF NOT EXISTS idx_edit_commit_datetime ON edit(commit_date_time)", 
           nativeQuery = true)
    void createCommitDateTimeIndexIfNotExists();


    @Query(value = "SELECT * FROM get_edit_pairs(:commitDateTime)", nativeQuery = true)
    List<Object[]> getRawEditPairsByCommitDateTime(@Param("commitDateTime") ZonedDateTime commitDateTime);
    
    default Set<Pair<String, String>> getEditPairsByCommitDateTime(ZonedDateTime commitDateTime) {
        List<Object[]> results = getRawEditPairsByCommitDateTime(commitDateTime);
        
        return results.stream()
                      .map(result -> new Pair<>((String) result[0], (String) result[1]))
                      .collect(Collectors.toSet());
    }


    public List<Edit> findAllByCommitDateTimeAndDatasourcePurlInOrCommitDateTimeBeforeAndDatasourcePurlInOrderByCommitDateTimeAsc(
        ZonedDateTime commitDateTime1,
        List<String> datasourcePurl1,
        ZonedDateTime commitDateTime2,
        List<String> datasourcePurl2
    );

    public List<Edit> findAllByCommitDateTime(ZonedDateTime commDateTime);

    
    @Modifying
    @Transactional
    @Query(value = "CALL update_edit_finding_counts(?1, ?2, ?3)", nativeQuery = true)
    void updateEditFindingCounts(String purlsString, ZonedDateTime commitDateTime, String datasourcePurl);


    @Modifying
    @Transactional
    @Query(value = "CALL create_edit_indexes()", nativeQuery = true)
    void creatEditIndexes();

}
