package io.patchfox.analyze_service.helpers;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;

import org.hibernate.Hibernate;
import org.springframework.beans.factory.annotation.Autowired;
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

}

