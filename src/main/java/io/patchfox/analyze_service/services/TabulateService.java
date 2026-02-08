package io.patchfox.analyze_service.services;


import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import com.github.packageurl.MalformedPackageURLException;
import com.github.packageurl.PackageURL;

import io.patchfox.analyze_service.components.EnvironmentComponent;
import io.patchfox.analyze_service.helpers.TabulateServiceTransactionalHelpers;
import io.patchfox.analyze_service.repositories.DatasetMetricsRepository;
import io.patchfox.analyze_service.repositories.DatasetRepository;
import io.patchfox.analyze_service.repositories.DatasourceEventRepository;
import io.patchfox.analyze_service.repositories.DatasourceEventJdbcRepository;
import io.patchfox.analyze_service.repositories.DatasourceMetricsRepository;
import io.patchfox.analyze_service.repositories.DatasourceRepository;
import io.patchfox.analyze_service.repositories.PackageRepository;
import io.patchfox.analyze_service.repositories.EditRepository;
import io.patchfox.analyze_service.repositories.FindingDataRepository;
import io.patchfox.analyze_service.repositories.FindingRepository;
import io.patchfox.db_entities.entities.DatasetMetrics;
import io.patchfox.db_entities.entities.Datasource;
import io.patchfox.db_entities.entities.DatasourceEvent;
import io.patchfox.db_entities.entities.Edit;
import io.patchfox.package_utils.json.ApiResponse;
import io.patchfox.package_utils.util.CvssSeverity;
import io.patchfox.package_utils.util.Pair;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

import lombok.extern.slf4j.Slf4j;


/*
 * 
 * 
 * TODO
 * 
 * redo all of this - as much as possible - using named queries
 * 
 * the db can be doing a lot of this for you 
 * 
 * vs having the app stack doing it 
 * 
 * https://docs.spring.io/spring-data/jpa/reference/jpa/query-methods.html
 * 
 * 
 * 
 * also ALL OF THIS IS SPAGHETTI - SORRY FUTURE ME ... 
 * 
 * 
 * 
 */


@Slf4j
@Service
public class TabulateService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    private ExecutorService executorService;

    @Autowired
    EnvironmentComponent env;

    @Autowired
    DatasetMetricsRepository datasetMetricsRepository;

    @Autowired
    DatasetRepository datasetRepository;

    @Autowired
    DatasourceRepository datasourceRepository;

    @Autowired
    DatasourceMetricsRepository datasourceMetricsRepository;

    @Autowired
    DatasourceEventRepository datasourceEventRepository; 

    @Autowired
    DatasourceEventJdbcRepository datasourceEventJdbcRepository;

    @Autowired
    PackageRepository packageRepository;

    @Autowired
    EditRepository editRepository;

    @Autowired
    FindingRepository findingRepository;

    @Autowired
    FindingDataRepository findingDataRepository;

    @Autowired
    TabulateServiceTransactionalHelpers tabulateServiceTransactionalHelpers; 

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    CacheService cacheService;

    enum BacklogTimeBucket {
        THIRTY_TO_SIXTY_DAYS,
        SIXTY_TO_NINETY_DAYS,
        NINETY_OR_MORE_DAYS
    } 
    

    /**
     * enumerate through ordered list of datasourceEvent record indexes, tabulate, and serialize to DatasetMetrics table
     * 
     * THIS METHOD ASSUMES THAT ALL ELEMENTS OF LIST datasourceEventIndexesByCommitDateAsc ARE FROM THE SAME DATASET
     * 
     * @param txid
     * @param requestReceivedAt
     * @param datasourceEventIndexesByCommitDateAsc
     * @return
     * @throws MalformedPackageURLException 
     */
    @Transactional
    public ApiResponse tabulate(
        UUID txid, 
        ZonedDateTime requestReceivedAt, 
        String datasetName,
        Integer pageIndex,
        Integer pageSize,
        List<Integer> datasourceEventIndexesByCommitDateAsc
    ) throws MalformedPackageURLException {

        // Cache for previous dataset metrics record ID
        Optional<Long> previousDatasetMetricsRecordId = Optional.empty();

        // Cache for previous datasource metrics record IDs by datasource PURL
        Map<String, Long> previousDatasourceMetricsRecordIdsByDatasourcePurl = new ConcurrentHashMap<>();

        var count = 0;
        var currentId = -1l;
        var fromIndex = pageIndex * pageSize;
        var toIndex = (fromIndex + pageSize) > datasourceEventIndexesByCommitDateAsc.size() 
                            ? datasourceEventIndexesByCommitDateAsc.size()
                            : fromIndex + pageSize;

        var datasourceEventIndexPage = datasourceEventIndexesByCommitDateAsc.subList(fromIndex, toIndex);

        log.info(
            "fromIndex: {}  toIndex: {}  datasourceEventIndexPage size: {}",
            fromIndex,
            toIndex,
            datasourceEventIndexPage.size()
        );

        var maxTabulateCacheSize = env.getMaxTabulateCacheSize();

        // find the dataset 
        // will fail if datasetname does not exist in db
        var datasetRecord = tabulateServiceTransactionalHelpers.getDatasetRecordForName(datasetName);
        log.info("dataset is: {}", datasetRecord.getName());

        List<Long> createdRecordIdsAscByDate = new ArrayList<Long>();
        List<Long> errorRecordIdsAscByDate = new ArrayList<Long>();

        // cache to keep track of what datasources have what package indexes as we enumerate and process events 
        var historicalPackagePurlsByDatasourcePurl = new ConcurrentHashMap<ZonedDateTime, Map<String, Set<String>>>();

        /*
         * 
         * this presently is only ever used by two methods. the first that handles updating the current package set 
         * and the one after that handles finding tabulation. the former will only ever look at the most recent (by
         * commitDateTime) record which has a full datasource-> finding map. the latter will look at all of them BUT
         * any record other than the HEAD (by commitDateTime) MAY NOT HAVE A FULL DTASOURCE->FINDING MAP but insteam
         * a map with one entry keyed to "HISTORICAL". The reason has to dow ith how caches are repopulated at the 
         * start of new page processing (there's a note in that block a bit down from here explaining more). tl'dr
         * 
         * don't trust that anything but the most recent (by commitdatetime) has a full datasource->finding map. anything
         * else might have it OR it might have a single entry called HISTORICAL to permit backlog finding tabulation 
         * which doesn't care about which datasource is attached to which finding.
         * 
         * be thee warned - thar be dragons here
         * 
         * 
         */
        var historicalFindingsByDatasourcePurl = new ConcurrentHashMap<ZonedDateTime, Map<String, Set<Pair<CvssSeverity, String>>>>();

        // delete all datasetMetrics records from the first commit date on. we're about to replace them all. we delete
        // and replace because we have a different set of facts from the point of the first commit date onward and thus
        // all the metrics need to be recomputed. 
        var firstEventIndex = datasourceEventIndexPage.get(0);
        var firstEventRecord = tabulateServiceTransactionalHelpers.getDatasourceEventRecordForId(Long.valueOf(firstEventIndex));
        
        var firstEventCommitDatetime = firstEventRecord.getCommitDateTime();

        // this will remove all germane records from all three metrics tables 
        tabulateServiceTransactionalHelpers.deleteMetricsByCommitDateTimeOrCommitDateTimeAfter(firstEventCommitDatetime);

        // now grab the metrics records for three months prior plus a few days
        var threeMonthsBeforeCommitDateTime = firstEventRecord.getCommitDateTime().minusDays(100);

        // TODO nerfing this because we no longer need to initialize the edit collection and thus hopefully
        // things will run a bit more quickly and efficiently
        //
        // var historicalDatasetMetricsRecordsByCommitDateAsc = 
        //     tabulateServiceTransactionalHelpers.getHistoricalDatasetMetricRecordsByEventDateAsc(threeMonthsBeforeCommitDateTime);

        // TODO we don't really need all the dsm records anymore - just the last event from this list 
        // if performance is shite at scale this is an area for improvment 
        //
        // TODO seriously we only grab the commitDateTime values so far as I can tell... 
        //
        // var historicalDatasetMetricsRecordsByCommitDateAsc =
        //     datasetMetricsRepository.findAllByIsCurrentAndCommitDateTimeBeforeOrderByCommitDateTimeAsc(
        //         true, firstEventRecord.getCommitDateTime(), maxTabulateCacheSize
        // );


        // Map<ZonedDateTime, Set<Edit>> historicalDatasetEditsByCommitDateAsc = new HashMap<>();
        Map<ZonedDateTime, Set<Pair<String, String>>> historicalDatasetEditsByCommitDateAsc = new ConcurrentHashMap<>();


        // cache to speed up record processing so we don't have to figure out if thousands of packages have findings 
        // in them at every record processing iteration. 
        Set<String> currentPackagePurlsWithFindings = new HashSet<String>();


        Optional<DatasetMetrics> currentHistoricalDatasetMetricsRecordOptional = Optional.empty();

        // ===== REDIS CACHE INTEGRATION: TRY LOAD FROM REDIS FIRST =====
        // Load cache from PREVIOUS page (pageIndex - 1) since current page hasn't been processed yet
        var cachedPackages = pageIndex > 0 ? cacheService.loadPackageCache(datasetName, pageIndex - 1) : null;
        var cachedFindings = pageIndex > 0 ? cacheService.loadFindingCache(datasetName, pageIndex - 1) : null;
        var cachedEdits = pageIndex > 0 ? cacheService.loadEditCache(datasetName, pageIndex - 1) : null;

        boolean cacheHit = (cachedPackages != null && cachedFindings != null && cachedEdits != null);

        // PERFORMANCE FIX: Only fetch historical metrics if cache miss - otherwise wasted work
        List<DatasetMetrics> historicalDatasetMetricsRecordsByCommitDateAsc = new ArrayList<>();
        
        if (!cacheHit) {
            log.info("*** REDIS CACHE MISS: Fetching historical metrics from database for dataset: {}, page: {} ***", datasetName, pageIndex);
            // pull the latest batch of records 
            var historicalDatasetMetricsRecordsByCommitDateDesc =
                tabulateServiceTransactionalHelpers.findAllByIsCurrentAndCommitDateTimeBeforeOrderByCommitDateTimeDesc(
                    true, firstEventRecord.getCommitDateTime(), maxTabulateCacheSize
            );
            historicalDatasetMetricsRecordsByCommitDateAsc = historicalDatasetMetricsRecordsByCommitDateDesc.reversed();
        }

        if (cacheHit) {
            log.info("*** REDIS CACHE HIT: Loaded all caches from Redis for dataset: {}, page: {} (from page {}) ***", datasetName, pageIndex, pageIndex - 1);
            historicalPackagePurlsByDatasourcePurl.putAll(cachedPackages);
            historicalFindingsByDatasourcePurl.putAll(cachedFindings);
            historicalDatasetEditsByCommitDateAsc.putAll(cachedEdits);
            
            // Populate currentPackagePurlsWithFindings - get all package PURLs from cached packages
            // that appear in the findings map (meaning they have findings)
            var allCachedPackagePurls = cachedPackages.values().stream()
                .flatMap(m -> m.values().stream())
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
            
            // Filter to only packages that have findings by checking if they're keys in the findings map
            currentPackagePurlsWithFindings = allCachedPackagePurls.stream()
                .filter(purl -> cachedFindings.values().stream()
                    .anyMatch(m -> m.containsKey(purl)))
                .collect(Collectors.toSet());
            
            log.info("Populated currentPackagePurlsWithFindings with {} packages from cache", currentPackagePurlsWithFindings.size());
        }

        if (!cacheHit && !historicalDatasetMetricsRecordsByCommitDateAsc.isEmpty() ) {
        //if ( !historicalDsmIdsAndCommitDateTimeAsc.isEmpty() ) {
            // because we want the oldest of the most recent [n] records NOT the oldest of the first [n] records. the 
            // latter creates problems that cause metrics to suddenly dip to near zero 
            var historicalDatasetMetricsRecord = historicalDatasetMetricsRecordsByCommitDateDesc.getFirst();
            //var historicalDatasetMetricsRecord = historicalDatasetMetricsRecordsByCommitDateAsc.getLast();
            var historicalCommitDateTime = historicalDatasetMetricsRecord.getCommitDateTime();
            //var historicalCommitDateTime = historicalDsmIdsAndCommitDateTimeAsc.getLast().getRight();
            log.info("**** historicalCommitDateTime is: {}", historicalCommitDateTime);
            currentHistoricalDatasetMetricsRecordOptional = Optional.of(historicalDatasetMetricsRecord);
            previousDatasetMetricsRecordId = Optional.of(historicalDatasetMetricsRecord.getId());

            // TODO this is all a bit of goober spaghetti code but tl'dr what's going on is we need to populate
            // these caches here so later when we compute datasourceMetrics records we know what the previous records
            // were. We can't assume the datasetMetrics records and the datasourceMetrics records will have parity 
            // where their ID is concerned AND the datasetMetrics table doesn't track the purl of the datasourceEvent that
            // populated it. Therefore we need to do some digging to get what we need. Upstream (orchestrate-service)
            // guarantees that commitDateTime is unique so we use that from the datasetMetrics record to get the 
            // datasourceEvent record that caused the datasetMetrics record to be created, then from there grab the 
            // datasource PURL and use that to find the most recent record tied to that datasource purl in the 
            // datasourceMetrics table. 
            // also - we know all of these records will be present because they would have been created at the same 
            // time the datasetMetrics record was created. 
            
            /*
            
            this is because of the fuckiest of fucky bugs. tl'dr - the logic in orchestrate service that 
            ensures commitDateTime is unique only applies to datasourceEvent records THAT ARE CURRENTLY IN FLIGHT

            meaning, on occasion, the call to find datasourceEvents by historical commitDateTime may pick up more than
            one record if you don't also filter based on status = READY_FOR_NEXT_PROCESSING - which indicates the
            datasourceMetricsRecord is IN FLIGHT and thus is guaranteed to be uniquely identified by commitDateTime 

            11JAN26 DH 
            */
            // for (var dse : datasourceEventRepository.findAllByCommitDateTime(historicalCommitDateTime)) {
            //     log.info(
            //         "dse from unfiltered dse call is: {} {} {}", 
            //         dse.getId(), 
            //         dse.getPurl(), 
            //         dse.getCommitDateTime());
            // }
            var previousDatasourceEvent = 
                datasourceEventRepository.findAllByCommitDateTimeAndStatus(
                    historicalCommitDateTime,
                    DatasourceEvent.Status.READY_FOR_NEXT_PROCESSING
                ).getFirst();
            log.info(
                "dse from filtered call is: {} {} {}", 
                previousDatasourceEvent.getId(), 
                previousDatasourceEvent.getPurl(), 
                previousDatasourceEvent.getCommitDateTime()
            );
            var previousDatasourcePurl = previousDatasourceEvent.getDatasource().getPurl();

            var previosDatasourceMetricsRecordId = 
                datasourceMetricsRepository.findFirstByPurlOrderByCommitDateTimeDesc(previousDatasourcePurl)
                                           .get()
                                           .getId();


            previousDatasourceMetricsRecordIdsByDatasourcePurl.put(previousDatasourcePurl, previosDatasourceMetricsRecordId);

            //var dsmRecordId = historicalDsmIdsAndCommitDateTimeAsc.getLast().getLeft();
            //currentHistoricalDatasetMetricsRecordOptional = Optional.of(datasetMetricsRepository.findById(dsmRecordId)).get();

            // TODO 
            // this actually pulls the CURRENT datasource purls for the dataset in question NOT the purls for the 
            // dataset as it was at the time. 
            // the net effect will be ok because datasources that weren't present at the time of historical 
            // commitDateTime will end up with empty package sets assocaited with them (because they didn't exist!)
            var currentDatasetRecordId = historicalDatasetMetricsRecord.getDataset().getId();
            log.info("currentDatasetRecordId is: {}", currentDatasetRecordId);
            //var historicalRecordId = historicalDsmIdsAndCommitDateTimeAsc.getLast().getLeft();
            var datasourcePurls = datasetRepository.getDatasourcePurls(currentDatasetRecordId);
            log.info("datasourcePurls size is (should never go down between pages!): {}", datasourcePurls.size());
            log.debug("datasourcePurls is: {}", datasourcePurls);

            var startPopulateCaches = Instant.now();

            /*
             * 
             * presently - in the main loop - this service is populating the package_ids field in each datasource 
             * record on each datasource_event processed. there are reasons for this explained in the comment of that 
             * section of code in this class. the fun part is that - because each datasource_event record has to be
             * processed by this logic in order of commit date time, we also know that no matter what so long as we 
             * get input in correct temporal (asc) ordr by commit date time the values in those datasource records are 
             * accurate in terms of what packages are currently associated with that datasource at the time just 
             * prior to the record we're currently processing. 
             * 
             * therefore - we don't need to go through the edits for each datasource and rebuild the current state
             * we can just fetch it from the database 
             * 
             */
            // var packagePurlsByDatasourcePurl = 
            //     getDatasetPackageSetForCommitDateTime(historicalCommitDateTime, datasourcePurls);
            var packagePurlsByDatasourcePurl = (ConcurrentHashMap<String, Set<String>>)getDatasetPackageSetForDatasources(datasourcePurls);

            historicalPackagePurlsByDatasourcePurl.put(historicalCommitDateTime, packagePurlsByDatasourcePurl);
            log.debug("historicalPackagePurlsByDatasourcePurl: {}", historicalPackagePurlsByDatasourcePurl);
            var donePopulatePurlCache = Instant.now();
            log.info("time to complete populatePurlCache: {}", Duration.between(startPopulateCaches, donePopulatePurlCache));

            /*
             * 
             * here we are replacing a slower method with one that throws the work to the executor service for async
             * processing
             * 
             */
            // var findingsByDatasourcePurl = 
            //     getDatasetFindingPairsByDatasourcePurlForCommitDateTime(historicalCommitDateTime, packagePurlsByDatasourcePurl);

            var findingsByDatasourcePurl = 
                (ConcurrentHashMap<String, Set<Pair<CvssSeverity, String>>>)
                    getDatasetFindingPairsByDatasourcePurlForCommitDateTime(
                        packagePurlsByDatasourcePurl, 
                        currentPackagePurlsWithFindings
                    );
            // log.debug("findingsByDatasourcePurl is: {}", findingsByDatasourcePurl);
             historicalFindingsByDatasourcePurl.put(historicalCommitDateTime, findingsByDatasourcePurl);



            /*
            * Cache population strategy:
            * 1. Include all records from the previous page (for continuity)
            * 2. Add specific records closest to 30/60/90 days prior to current processing date
            * 3. This ensures proper historical coverage for backlog calculations regardless of page boundaries
            */

            // Get current processing date (first record of current page)
            var currentDatasourceEventRecord = 
                    tabulateServiceTransactionalHelpers.getDatasourceEventWithPackagesForId(datasourceEventIndexPage.get(0));
    
            var currentProcessingDate = currentDatasourceEventRecord.getCommitDateTime();

            var thirtyDaysAgo = currentProcessingDate.minusDays(30);
            var sixtyDaysAgo = currentProcessingDate.minusDays(60);
            var ninetyDaysAgo = currentProcessingDate.minusDays(90);

            // // Collect records to cache
            // var recordsToCache = new HashSet<DatasetMetrics>();

            // // 1. Add all records from previous page (if we're not on the first page)
            // var startOfCurrentPage = fromIndex; // This should be the start index of current page
            // if (startOfCurrentPage > 0) {
            //     var previousPageStart = Math.max(0, startOfCurrentPage - pageSize);
            //     var previousPageEnd = startOfCurrentPage;
                
            //     for (int i = previousPageStart; i < previousPageEnd; i++) {
            //         if (i < historicalDatasetMetricsRecordsByCommitDateAsc.size()) {
            //             recordsToCache.add(historicalDatasetMetricsRecordsByCommitDateAsc.get(i));
            //         }
            //     }
            // }

            // // 2. Add records closest to target dates (30/60/90 days ago)
            // var targetDates = List.of(thirtyDaysAgo, sixtyDaysAgo, ninetyDaysAgo);

            // for (var targetDate : targetDates) {
            //     var closestRecord = historicalDatasetMetricsRecordsByCommitDateAsc
            //         .stream()
            //         .filter(record -> record.getCommitDateTime().isBefore(targetDate))
            //         .min((r1, r2) -> {
            //             var diff1 = Math.abs(ChronoUnit.DAYS.between(r1.getCommitDateTime(), targetDate));
            //             var diff2 = Math.abs(ChronoUnit.DAYS.between(r2.getCommitDateTime(), targetDate));
            //             return Long.compare(diff1, diff2);
            //         });
                
            //     closestRecord.ifPresent(recordsToCache::add);
            // }


            // Collect records to cache
            log.info("begin populate historical findings and package caches");
            //var recordsToCache = new HashSet<Pair<Long, ZonedDateTime>>();
            var recordsToCache = new HashSet<DatasetMetrics>();

            // 1. FIRST: Ensure we have 30/60/90 day coverage
            var targetDates = List.of(thirtyDaysAgo, sixtyDaysAgo, ninetyDaysAgo);
            for (var targetDate : targetDates) {

                // var closestRecord = 
                //     historicalDsmIdsAndCommitDateTimeAsc.stream()
                //                                         .filter(record -> record.getRight().isBefore(targetDate))
                //                                         .max(Comparator.comparing(record -> record.getRight()));


                var closestRecord = 
                    historicalDatasetMetricsRecordsByCommitDateAsc.stream()
                                                                  .filter(record -> record.getCommitDateTime().isBefore(targetDate))
                                                                  .max(Comparator.comparing(DatasetMetrics::getCommitDateTime));
                
                closestRecord.ifPresent(record -> {
                    log.info("Adding {} day record: {}", 
                        ChronoUnit.DAYS.between(record.getCommitDateTime(), currentProcessingDate), 
                        record.getCommitDateTime());
                        // ChronoUnit.DAYS.between(record.getRight(), targetDate), 
                        // record.getRight());
                    recordsToCache.add(record);
                });
            }

            // 2. THEN: Add previous page records for continuity (these may overlap, but Set dedupes)
            var startOfCurrentPage = fromIndex; // This should be the start index of current page
            if (startOfCurrentPage > 0) {
                var previousPageStart = Math.max(0, startOfCurrentPage - pageSize);
                var previousPageEnd = startOfCurrentPage;
                
                for (int i = previousPageStart; i < previousPageEnd; i++) {
                    if (i < historicalDatasetMetricsRecordsByCommitDateAsc.size()) {
                        recordsToCache.add(historicalDatasetMetricsRecordsByCommitDateAsc.get(i));
                    }
                }
                // for (int i = previousPageStart; i < previousPageEnd; i++) {
                //     if (i < historicalDsmIdsAndCommitDateTimeAsc.size()) {
                //         recordsToCache.add(historicalDsmIdsAndCommitDateTimeAsc.get(i));
                //     }
                // }
            }

            log.info("Final cache records: {}", recordsToCache.size());
            log.debug("packagePurlsByDatasourcePurl: {}", packagePurlsByDatasourcePurl);

            // this temp business is almost certainly not needed anymore but things are finally working and I don't want
            // to fuck with it anymore. tl'dr - the issue was the jdbc loading of currentDatasourceEvent was causing 
            // the hibernate-managed relation between datasource and dataset to be nerfed and when later in processing
            // flow we tried to use it we ended up stomping on that value and thus, at the begining of the next
            // processing page when we tried to get a list of datasources associated with the dataset, the ones that
            // were processed in the previous page were not showing up because we'd stomped on the relation. This was
            // resulting in a condition where it looked for a while like the caches were getting corrupted by this 
            // runnable somehow (they weren't) - hence the temp shit. 
            //var historicalPackagePurlsByDatasourcePurlTEMP = new ConcurrentHashMap<ZonedDateTime, Map<String, Set<String>>>();
            //var historicalFindingsByDatasourcePurlTEMP = new ConcurrentHashMap<ZonedDateTime, Map<String, Set<Pair<CvssSeverity, String>>>>();


            // 3. Populate cache for all selected records (but skip if already cached - we already added the last event 
            //    from the previous page and it has richer data than some of what follows)
            var futures = new ArrayList<Future<?>>();
            recordsToCache.forEach(record -> {
                Runnable r = () -> {
                    // checking only package cache because finding cache will only ever be populated if package cache
                    // is also populated 
                    if (record.getCommitDateTime() != historicalCommitDateTime && !historicalPackagePurlsByDatasourcePurl.containsKey(record.getCommitDateTime())) {
                    //if ( !historicalFindingsByDatasourcePurl.containsKey(record.getRight()) ) {
                        log.info("updating historical findings and package cache for commit datetime: {}", record.getCommitDateTime());
                        String packageIdsString = listLongToSqlArrayString(record.getPackageIndexes());
                        var packagePurls = packageRepository.filterPackageIdsWithFindings(packageIdsString);
                        var findingsPairs = new HashSet<>(findingRepository.findSeverityAndIdentifierByPackagePurlIds(packagePurls, new HashSet<>()));
                        historicalFindingsByDatasourcePurl.put(record.getCommitDateTime(), Map.of("HISTORICAL", findingsPairs));
                        // log.info("updating historicalFindings cache for commit datetime: {}", record.getRight());
                        // var dsmRecord = datasetMetricsRepository.findById(record.getLeft()).get();
                        // String packageIdsString = listLongToSqlArrayString(dsmRecord.getPackageIndexes());
                        // var packagePurls = packageRepository.filterPackageIdsWithFindings(packageIdsString);
                        // var findingsPairs = new HashSet<>(findingRepository.findSeverityAndIdentifierByPackagePurlIds(packagePurls, new HashSet<>()));
                        // historicalFindingsByDatasourcePurl.put(dsmRecord.getCommitDateTime(), Map.of("HISTORICAL", findingsPairs));
                        
                        // Group packages by datasource (we need to reconstruct the datasource->packages map)
                        // For simplicity, we can put all packages under a single "HISTORICAL" key similar to findings
                        // OR better: fetch datasource info for each package and group properly
                        var allPackagePurls = packageRepository.findPurlsByIds(packageIdsString); // Need all packages, not just ones with findings
                        var packageMap = new ConcurrentHashMap<String, Set<String>>();
                        packageMap.put("HISTORICAL", new HashSet<String>(allPackagePurls));
                        historicalPackagePurlsByDatasourcePurl.put(record.getCommitDateTime(), packageMap);
                    } 
                    // else {
                    //     log.info("in runnable key: {}  bucket contents size: {}", record.getCommitDateTime(), historicalPackagePurlsByDatasourcePurl.get(record.getCommitDateTime()).values().stream().flatMap(x -> x.stream()).toList().size());
                    // }
                };
                futures.add(executorService.submit(r));
            });
            // all of this nonsense to block execution until async tasks are complete ... 
            futures.stream().map(f -> {
                try {
                    return f.get();
                } catch (InterruptedException | ExecutionException e) {
                    log.error("caught exception while building up list of edits", e);
                }
                return null;
            }).toList();


            for (var entrySet : historicalPackagePurlsByDatasourcePurl.entrySet()) {
                log.info("historicalPackagePurlsByDatasourcePurl key: {}  bucket contents size: {}", entrySet.getKey(), entrySet.getValue().values().stream().flatMap(x -> x.stream()).toList().size());
                //historicalPackagePurlsByDatasourcePurl.put(entrySet.getKey(), entrySet.getValue());
            }
            for (var entrySet : historicalFindingsByDatasourcePurl.entrySet()) {
                log.info("historicalFindingsByDatasourcePurl key: {}  bucket contents size: {}", entrySet.getKey(), entrySet.getValue().values().stream().flatMap(x -> x.stream()).toList().size());
                //historicalFindingsByDatasourcePurl.put(entrySet.getKey(), entrySet.getValue());
            }
            log.debug("historicalPackagePurlsByDatasourcePurl: {}", historicalPackagePurlsByDatasourcePurl);
            log.debug("historicalFindingsByDatasourcePurl is: {}", historicalFindingsByDatasourcePurl);

            var donePopulateFindingCache = Instant.now();
            log.info("time to complete populate finding and package caches: {}", Duration.between(donePopulatePurlCache, donePopulateFindingCache));


            // populate edit cache
            // throwing this to the worker pool because at scale each one of these takes about a minute 
            // in case the index doesn't already exist we need to create it. we do this here because it's a locking
            // operation and if we put this in the runnable call it nerfs all the async work
            
            
            // don't do this here 
            //editRepository.createCommitDateTimeIndexIfNotExists();

            // we don't need to populate the totality of the edit cache - we only need the last ninety days 
            var ninetyDaySubCach = 
                historicalDatasetMetricsRecordsByCommitDateAsc.stream()
                                                              .filter(record -> record.getCommitDateTime().isAfter(ninetyDaysAgo))
                                                              .toList();

            // clear our futures from findings cache work
            futures.clear();
            for (var dsm : ninetyDaySubCach) {
            //for (var dsmPair : historicalDsmIdsAndCommitDateTimeAsc) {
                var cdt = dsm.getCommitDateTime();
                //var cdt = dsmPair.getRight();

                // doing this in serial vs using the thread pool because it seems as though doing it in the thread pool
                // constipates the database in a way such that there's lock contention and the first record of the page
                // ends up taking a lot longer to process 
                //
                // the transactionTemplate creates a completely separate transaction and ensures the connection is 
                // closed upon completion 
                Set<Pair<String, String>> editPairs = transactionTemplate.execute(status -> {
                    return jdbcTemplate.query(
                        "SELECT * FROM get_edit_pairs(?::TIMESTAMP WITH TIME ZONE)",
                        (rs, rowNum) -> new Pair<>(rs.getString(1), rs.getString(2)),
                        cdt.toString()
                    ).stream().collect(Collectors.toSet());
                    // Transaction commits automatically when this lambda returns
                    // Connection is released back to pool immediately
                });
                
                historicalDatasetEditsByCommitDateAsc.put(cdt, editPairs);

            //     Runnable r = () -> { 
            //         log.info("updating edit cache for commit datetime: {}", cdt);
            //         // Set<Pair<String, String>> editPairs = editRepository.getEditPairsByCommitDateTime(cdt);
            //         // historicalDatasetEditsByCommitDateAsc.put(cdt, editPairs);
    
            //         // // Direct JDBC call - much faster than Hibernate
            //         // Set<Pair<String, String>> editPairs = jdbcTemplate.query(
            //         //     "SELECT * FROM get_edit_pairs(?::TIMESTAMP WITH TIME ZONE)",
            //         //     (rs, rowNum) -> new Pair<>(rs.getString(1), rs.getString(2)),
            //         //     cdt.toString()
            //         // ).stream().collect(Collectors.toSet());

            //         // This creates a completely separate transaction
            //         Set<Pair<String, String>> editPairs = transactionTemplate.execute(status -> {
            //             return jdbcTemplate.query(
            //                 "SELECT * FROM get_edit_pairs(?::TIMESTAMP WITH TIME ZONE)",
            //                 (rs, rowNum) -> new Pair<>(rs.getString(1), rs.getString(2)),
            //                 cdt.toString()
            //             ).stream().collect(Collectors.toSet());
            //             // Transaction commits automatically when this lambda returns
            //             // Connection is released back to pool immediately
            //         });
                    
            //         historicalDatasetEditsByCommitDateAsc.put(cdt, editPairs);
            //     };
            //     futures.add(executorService.submit(r));
            
            }

            // // all of this nonsense to block execution until async tasks are complete ... 
            // futures.stream().map(f -> {
            //     try {
            //         return f.get();
            //     } catch (InterruptedException | ExecutionException e) {
            //         log.error("caught exception while building up list of edits", e);
            //     }
            //     return null;
            // }).toList();


            //historicalDsmIdsAndCommitDateTimeAsc.clear();

            var donePopulateEditCache = Instant.now();
            log.info("time to complete populateEditCache: {}", Duration.between(donePopulateFindingCache, donePopulateEditCache));

            historicalDatasetMetricsRecordsByCommitDateAsc.clear();
            entityManager.flush();
            entityManager.clear();
            log.info("historicalDatasetMetricsRecordsByCommitDateAsc and entityManager flushed and cleared");

            log.info(">>>>>>>>>> total time to populate chaches: {}", Duration.between(startPopulateCaches, donePopulateEditCache));
        } // end cache hydration block
        // ===== END REDIS CACHE INTEGRATION =====

        // 

        for (var datasourceEventIndex : datasourceEventIndexPage) {
            var startProcessingRecord = Instant.now();
            try {
                count += 1;
                currentId = datasourceEventIndex;


                // TRYING TO SOLVE A BOTTLENECK THAT HAPPENS AT THE TOP OF EVERY PAGE  - MEANING THE FIRST RECORD 
                // OF A PAGE IS ALWAYS TAKING TOO LONG - DON'T KNOW WHY EXCEPT THAT HIBERNATE DOING SOMETHING FUCKY 
                // AND/OR CONNECTION CONTENTION AT THE END OF CACHE POPULATION PROCESSING 
                // 
                // EITHER WAY findByIdWithPackagesImpl should at least eliminate any hibernate n+1 queries as it tries to 
                // populate collections 
                //
                // ** DOES NOT FULLY POPULATE DATASOURCE OBJ INSO FAR AS THE LINKAGE BETWEEN IT AND DATASET **
                var currentDatasourceEventRecord = 
                    datasourceEventJdbcRepository.findByIdFullyHydrated(Long.valueOf(datasourceEventIndex));

                    //eateDatasourceEventFromJdbcSingleQuery(datasourceEventIndex);
                    //datasourceEventRepository.findByIdWithPackagesImpl(Long.valueOf(datasourceEventIndex));
                    //tabulateServiceTransactionalHelpers.getDatasourceEventWithPackagesForId(datasourceEventIndex);
    
                var currentCommitDateTime = currentDatasourceEventRecord.getCommitDateTime();

                // quirk of too much refactoring - this is always going to be one 
                // TODO clean this out when you do the big refactor 
                var historicalEventCount = currentHistoricalDatasetMetricsRecordOptional.isEmpty() 
                                            ? 1
                                            : currentHistoricalDatasetMetricsRecordOptional.get().getDatasourceEventCount() + 1;

                log.info(
                    "processing record txid: {}  commitDatetime: {}  purl: {}", 
                    currentDatasourceEventRecord.getTxid(), 
                    currentDatasourceEventRecord.getCommitDateTime(),
                    currentDatasourceEventRecord.getPurl()
                );

                // see https://patchfox-workspace.slack.com/archives/C067UNTG350/p1741384967810699
                // if (historicalPackagePurlsByDatasourcePurl.containsKey(currentDatasourceEventRecord.getCommitDateTime())) {
                //     log.warn("*** skipping record due to previously processed record with same commitDateTime ***");
                //     continue;
                // }
    
                // create but do not save a new DatasetMetrics record 
                var datasetMetricsRecord = DatasetMetrics.builder()
                                                         .txid(currentDatasourceEventRecord.getTxid())
                                                         .jobId(currentDatasourceEventRecord.getJobId())
                                                         .commitDateTime(currentDatasourceEventRecord.getCommitDateTime())
                                                         .eventDateTime(currentDatasourceEventRecord.getEventDateTime())
                                                         .datasourceCount(datasetRepository.getDatasourceCount(datasetRecord.getId()))
                                                         .datasourceEventCount(historicalEventCount)
                                                         //.dataset(datasetRepository.save(datasetRecord))
                                                         .dataset(datasetRecord)
                                                         .packageFamilies(new HashSet<String>())
                                                         .edits(new HashSet<Edit>())
                                                         .isCurrent(true)
                                                         .patches(0)
                                                         .samePatches(0)
                                                         .patchFoxPatches(0)
                                                         .downlevelPackages(0)
                                                         .downlevelPackagesMajor(0)
                                                         .downlevelPackagesMinor(0)
                                                         .downlevelPackagesPatch(0)
                                                         .stalePackages(0)
                                                         .stalePackagesSixMonths(0)
                                                         .stalePackagesOneYear(0)
                                                         .stalePackagesOneYearSixMonths(0)
                                                         .stalePackagesTwoYears(0)
                                                         .build();
    
                log.info("base dsm record created. starting record processing run...");

                //
                datasetMetricsRecord = updateCachesAndMetricsRecordWithPackageEdits(
                    historicalFindingsByDatasourcePurl,
                    historicalPackagePurlsByDatasourcePurl, 
                    currentDatasourceEventRecord,
                    datasetMetricsRecord,
                    historicalDatasetEditsByCommitDateAsc
                );
    
                log.info("done updateCachesAndMetricsRecordWithPackageEdits");
        

                //  handle all the package and findings and findings backlog tabulation
                datasetMetricsRecord = updateDatasetMetricsRecordWithCve(
                    currentDatasourceEventRecord,
                    historicalFindingsByDatasourcePurl,
                    historicalPackagePurlsByDatasourcePurl,
                    historicalDatasetEditsByCommitDateAsc, 
                    datasetMetricsRecord,
                    currentPackagePurlsWithFindings
                );
                log.info("done updateDatasetMetricsRecordWithCve");

               log.debug("datasetMetricsRecord patches after updateDatasetMetricsRecordWithCve is: {}", datasetMetricsRecord.getPatches());

                // handle RPS and PES tabluations 
                datasetMetricsRecord = updateDatasetMetricsRecordWithRpsAndPes(
                    //historicalFindingsByDatasourcePurl.get(currentCommitDateTime),
                    //historicalPackagePurlsByDatasourcePurl.get(currentCommitDateTime),
                    //historicalFindingsByDatasourcePurl,
                    historicalPackagePurlsByDatasourcePurl,
                    datasetMetricsRecord,
                    currentHistoricalDatasetMetricsRecordOptional
                );
                log.info("done updateDatasetMetricsRecordWithRpsAndPes");
                
                // add indexes for all packages associated with this dataset_metrics record
                var datasourcePurlToPackagePurlMap = historicalPackagePurlsByDatasourcePurl.get(currentCommitDateTime);

                var purlList = datasourcePurlToPackagePurlMap.values()
                                                             .stream()
                                                             .flatMap(x -> x.stream())
                                                             .toList();

                var packageIndexes = packageRepository.getIdsForPurls(listToSqlArrayString(purlList));
                datasetMetricsRecord.setPackageIndexes(packageIndexes);
                datasetMetricsRecord.setPackages(packageIndexes.size());


                //
                // to future Dave from present Dave
                //
                // this is here because we need package indexes at both the dataset and datasource level 
                //
                // we're doing this here AND in the input service 
                // something is squirrly about what the input service is doing and we're 2 days from beta launch
                // and there's no time to figure out why. also our current exported ingest data from (github dataset)
                // does not have this information - so it needs to be included here so the recommend service has it. 
                // 
                // so for now we're doing the same thing twice. that's your (future) Dave's problem. 
                //
                // PS - just figured out what's going on with input service 
                // input-service doesn't necessarily get events in temporal order 
                // analyze has to handle this - or input has to work differently
                // mystery solved. 
                //var datasourceRecord = currentDatasourceEventRecord.getDatasource();
                var datasourceRecord = datasourceRepository.findById(currentDatasourceEventRecord.getDatasource().getId()).get();
                var datasourcePurlList = datasourcePurlToPackagePurlMap.get(datasourceRecord.getPurl())
                                                                       .stream()
                                                                       .toList();

                var datasourcePackageIndexes = packageRepository.getIdsForPurls(listToSqlArrayString(datasourcePurlList));
                datasourceRecord.setPackageIndexes(datasourcePackageIndexes);
                // this should not be necessary but w/o it hibernate breaks the assocation between datasource and dataset
                // and it makes baby kittens cry and die a horrible death and why do you want to cause hurt to a kitty? 
                // ok maybe not kitty death but it does make it so we can't see what datasources are attached to a given
                // dataset anymore and that breaks all the things. So we force hibernate to load the collection so 
                // it doesn't stomp on it when we serialize the datasource record. 
                log.info("dataset assocations are: {}", datasourceRecord.getDatasets().stream().map(d -> d.getName()).toList());
                datasourceRecord = datasourceRepository.save(datasourceRecord);

                // update datasourceEvent record to mark it as analyzed
                currentDatasourceEventRecord.setAnalyzed(true);
                currentDatasourceEventRecord.setStatus(DatasourceEvent.Status.READY_FOR_NEXT_PROCESSING);
                //datasourceEventRepository.save(currentDatasourceEventRecord);
                datasourceEventJdbcRepository.updateDatasourceEvent(currentDatasourceEventRecord);

                // and save the dataset metrics record 
                datasetMetricsRecord = datasetMetricsRepository.save(datasetMetricsRecord);   

                log.debug("datasetMetricsRecord patches near end is: {}", datasetMetricsRecord.getPatches());

                // now update package-index enrichment data in that dataset_metrics record by way of db stored procedure 
                var packageSet = historicalPackagePurlsByDatasourcePurl.get(currentCommitDateTime)
                                                                       .values()
                                                                       .stream()
                                                                       .flatMap(x -> x.stream())
                                                                       .collect(Collectors.toSet());

                // Break the package processing into manageable batches
                int batchSize = 500; // Adjust based on testing
                List<String> packageList = new ArrayList<>(packageSet);
                log.info(
                    "Processing {} packages for dataset metrics ID {}", 
                    packageList.size(), 
                    datasetMetricsRecord.getId()
                );

                for (int i = 0; i < packageList.size(); i += batchSize) {
                    int endIndex = Math.min(i + batchSize, packageList.size());
                    List<String> batch = packageList.subList(i, endIndex);
                    
                    try {
                        packageRepository.tabulatePackageIndexDataBatched(
                            setToSqlArrayString(new HashSet<>(batch)),
                            datasetMetricsRecord.getId(),
                            ","
                        );
                        
                        // Clear entity manager periodically to reduce memory pressure
                        //if (i % 2000 == 0 && i > 0) {
                            entityManager.clear();
                            System.gc();
                        //}
                    } catch (Exception e) {
                        log.error("Error processing package batch {}-{}: {}", i, endIndex, e.getMessage());
                        // Consider whether to throw the exception or continue with next batch
                    }
                }
                
                currentHistoricalDatasetMetricsRecordOptional = Optional.of(datasetMetricsRecord);
                createdRecordIdsAscByDate.add(datasetMetricsRecord.getId());      



                // create the datasourceMetrics record
                var currentDatasourcePurl = datasourceRecord.getPurl();

                var previousDatasourceMetricsRecordId = 
                    Optional.ofNullable(previousDatasourceMetricsRecordIdsByDatasourcePurl.get(currentDatasourcePurl));

                var datasourceMetricsRecordId = createDatasourceMetricsRecord(
                    currentDatasourceEventRecord.getId(),
                    datasetMetricsRecord.getId(),
                    previousDatasetMetricsRecordId,
                    previousDatasourceMetricsRecordId
                );      
                log.info("created datasourceMetricsRecord with id: {}", datasourceMetricsRecordId);
                previousDatasetMetricsRecordId = Optional.of(datasetMetricsRecord.getId());
                previousDatasourceMetricsRecordIdsByDatasourcePurl.put(datasourceRecord.getPurl(), datasourceMetricsRecordId);


                // create datasourceMetricsCurrent record
                createDatasourceMetricsCurrentRecord(datasourceMetricsRecordId);


                // var edits = editRepository.findAllByCommitDateTime(datasetMetricsRecord.getCommitDateTime());
                // var editPairs = edits.stream()
                //                      .map(e -> new Pair<>(e.getBefore(), e.getAfter()))
                //                      .collect(Collectors.toSet());

                // // var editPairs = datasetMetricsRecord.getEdits().stream()
                // //                                                .map(e -> new Pair<>(e.getBefore(), e.getAfter()))
                // //                                                .collect(Collectors.toSet());

                // historicalDatasetEditsByCommitDateAsc.put(
                //     datasetMetricsRecord.getCommitDateTime(), 
                //     editPairs
                //     //datasetMetricsRecord.getEdits().values().stream().
                // );


                log.info("historicalDatasetEditsByCommitDateAsc size: {}", historicalDatasetEditsByCommitDateAsc.size());
                log.info("historicalPackagePurlsByDatasourcePurl size is: {}", historicalPackagePurlsByDatasourcePurl.size());
                log.info("historicalFindingsByDatasourcePurl size is: {}", historicalFindingsByDatasourcePurl.size());
 
                var ninetyDaysPriorCommit = datasetMetricsRecord.getCommitDateTime().minusDays(90);

                // // // before we do anything else we need to make sure we retain the latest commit for any given datasource
                // // // so we don't lose the package information and thus fuck up our numbers 
                // var datasourcePurlToCommitDateTimeMap = new HashMap<String, List<ZonedDateTime>>();
                // for (var e : historicalPackagePurlsByDatasourcePurl.entrySet()) {
                //     var commitDateTime = e.getKey();
                //     // 99.99999% of the time there will only be one datasourceName per commit timestamp
                //     var datasourceNames = e.getValue().keySet();
                //     for (var datasourceName : datasourceNames) {
                //         if ( !datasourcePurlToCommitDateTimeMap.containsKey(datasourceName)) {
                //             datasourcePurlToCommitDateTimeMap.put(
                //                 datasourceName, 
                //                 new ArrayList<>(List.of(commitDateTime)) // because List.of() creates an immutable list
                //             );
                //         } else {
                //             datasourcePurlToCommitDateTimeMap.get(datasourceName).add(commitDateTime);
                //         }
                //     }
                // }

                var packageFamilyMap = new ConcurrentHashMap<String, Set<String>>();
                // var datasourcePurlToPackagePurlMap = historicalPackagePurlsByDatasourcePurl.get(currentCommitDateTime);
                // //for (var datasourcePurlToPackagePurlMap : historicalPackagePurlsByDatasourcePurl.values()) {

                // var purlList = datasourcePurlToPackagePurlMap.values()
                //                                              .stream()
                //                                              .flatMap(x -> x.stream())
                //                                              .toList();
                //log.info("purlList is: {}", purlList);
                for (var purl : purlList) {
                    var purlNoVersion = purl.split("@")[0]; 
                    if ( !packageFamilyMap.containsKey(purlNoVersion) ) { 
                        packageFamilyMap.put(purlNoVersion, new HashSet<String>()); 
                    }
                    packageFamilyMap.get(purlNoVersion).add(purl);
                }

            //}
            var totalPackageTypeCount = packageFamilyMap.values()
                                                        .stream()
                                                        .map(x -> x.size())
                                                        .reduce(0, (s, e) -> s + e);
            //log.info("packageFamily keys are: {}", packageFamilyMap.keySet());
            var packageFamilyCount = packageFamilyMap.keySet().size();

            log.info("for commit datetime: {}  totalPacakgeTypeCount: {}", currentCommitDateTime, totalPackageTypeCount);

                var done = false;
                while (!done) {
                    var earliestCommit = 
                        historicalDatasetEditsByCommitDateAsc.keySet()
                                                             .stream()
                                                             .sorted((x1, x2) -> x1.compareTo(x2))
                                                             .findFirst()
                                                             .get();

                    // var datasourcesWithOneCommitInCache = 
                    //     historicalPackagePurlsByDatasourcePurl.get(earliestCommit)
                    //                                           .keySet()
                    //                                           .stream()
                    //                                           .map(x -> datasourcePurlToCommitDateTimeMap.get(x).size())
                    //                                           .filter(x -> x < 2)
                    //                                           .toList();

                    // // intention here is to ensure enumerate all commit dates, starting from first to last, and 
                    // // ensure we don't remove the HEAD (ie - last remaining) commit and thus erase the datasource 
                    // // from cache. 
                    // //
                    // // TODO this scheme will fail to remove anything if "earliest commit" falls on the last commit 
                    // // for a given datasource. The > 90days check is currently commented out 
                    // boolean okToRemove = datasourcesWithOneCommitInCache.isEmpty();
                    if (earliestCommit.isBefore(ninetyDaysPriorCommit) && historicalDatasetEditsByCommitDateAsc.size() > maxTabulateCacheSize) {//&& okToRemove) { 
                        log.debug("purlList is: {}", purlList);
                        // log.info("historicalDatasetEditsByCommitDateAsc: {}", historicalDatasetEditsByCommitDateAsc);
                        // log.info("historicalPackagePurlsByDatasourcePurl: {}", historicalPackagePurlsByDatasourcePurl);
                        // log.info("historicalFindingsByDatasourcePurl: {}", historicalFindingsByDatasourcePurl);
                        //
                        //
                        // historicalDatasetEditsByCommitDateAsc getting mangled somehow after cache clear? 
                        // 
                        //
                        //

                        log.info(
                            "removing historical cache records with commitDateTime {} from cache that's " +
                            "> 90days from current event being processed",
                            earliestCommit
                        );

                        historicalDatasetEditsByCommitDateAsc.remove(earliestCommit); 
                        historicalPackagePurlsByDatasourcePurl.remove(earliestCommit);
                        historicalFindingsByDatasourcePurl.remove(earliestCommit);

                        // // dig into reverse cache and remove the commit 
                        // historicalPackagePurlsByDatasourcePurl.get(earliestCommit)
                        //                                       .keySet()
                        //                                       .stream()
                        //                                       .forEach(
                        //                                         x -> 
                        //                                         datasourcePurlToCommitDateTimeMap.get(x)
                        //                                                                          .remove(earliestCommit)
                        //                                       );

                    } else if (historicalDatasetEditsByCommitDateAsc.size() > maxTabulateCacheSize) {// && okToRemove) {

                        var numberToRemove = historicalDatasetEditsByCommitDateAsc.size() - maxTabulateCacheSize;

                        log.info(
                            "Cache size ({}) exceeds max ({}), need to remove {} records",
                            historicalDatasetEditsByCommitDateAsc.size(),
                            maxTabulateCacheSize,
                            numberToRemove
                        );

                        // Only evict entries older than 90 days from current processing date
                        // to preserve data needed for backlog calculation
                        var historicalCommitDateTimes =
                            historicalDatasetEditsByCommitDateAsc.keySet()
                                                                 .stream()
                                                                 .filter(dt -> dt.isBefore(ninetyDaysPriorCommit))
                                                                 .sorted((x1, x2) -> x1.compareTo(x2))
                                                                 .toList();

                        if (historicalCommitDateTimes.size() >= numberToRemove) {
                            // We have enough old entries (>90 days) to remove
                            var entriesToRemove = historicalCommitDateTimes.subList(0, numberToRemove);
                            log.info(
                                "Removing {} records with commitDateTimes older than 90 days (earliest: {}, latest removed: {})",
                                entriesToRemove.size(),
                                entriesToRemove.get(0),
                                entriesToRemove.get(entriesToRemove.size() - 1)
                            );

                            for (var historicalCommitDateTime : entriesToRemove) {
                                historicalDatasetEditsByCommitDateAsc.remove(historicalCommitDateTime);
                                historicalPackagePurlsByDatasourcePurl.remove(historicalCommitDateTime);
                                historicalFindingsByDatasourcePurl.remove(historicalCommitDateTime);
                            }
                        } else {
                            // Not enough old entries to remove - must keep cache oversized to preserve backlog data
                            log.warn(
                                "Cannot evict {} entries while preserving 90-day lookback window. " +
                                "Only {} entries are older than 90 days. Keeping cache oversized at {}. " +
                                "Consider increasing max-tabulate-cache-size configuration.",
                                numberToRemove,
                                historicalCommitDateTimes.size(),
                                historicalDatasetEditsByCommitDateAsc.size()
                            );
                            // Can't evict - break the loop to avoid infinite retries
                            done = true;
                        }


                    }
                    else { 
                        done = true; 
                        log.debug("historicalDatasetEditsByCommitDateAsc now: {}", historicalDatasetEditsByCommitDateAsc);
                        log.debug("historicalPackagePurlsByDatasourcePurl is now: {}", historicalPackagePurlsByDatasourcePurl);
                        log.debug("historicalFindingsByDatasourcePurl is now: {}", historicalFindingsByDatasourcePurl);

                        packageFamilyMap = new ConcurrentHashMap<String, Set<String>>();
                        datasourcePurlToPackagePurlMap = historicalPackagePurlsByDatasourcePurl.get(currentCommitDateTime);
                        //for (var datasourcePurlToPackagePurlMap : historicalPackagePurlsByDatasourcePurl.values()) {

                            purlList = datasourcePurlToPackagePurlMap.values()
                                                                    .stream()
                                                                    .flatMap(x -> x.stream())
                                                                    .toList();
                        //log.info("purlList is: {}", purlList);
                            for (var purl : purlList) {
                                var purlNoVersion = purl.split("@")[0]; 
                                if ( !packageFamilyMap.containsKey(purlNoVersion) ) { 
                                    packageFamilyMap.put(purlNoVersion, new HashSet<String>()); 
                                }
                                packageFamilyMap.get(purlNoVersion).add(purl);
                            }

                        //}
                        var NEWtotalPackageTypeCount = packageFamilyMap.values()
                                                                    .stream()
                                                                    .map(x -> x.size())
                                                                    .reduce(0, (s, e) -> s + e);

                        log.info("for commit datetime: {}  NEWtotalPackageTypeCount: {}", currentCommitDateTime, NEWtotalPackageTypeCount);
                        if (NEWtotalPackageTypeCount < totalPackageTypeCount) {
                            log.error("something went wrong - culling historical records should not have nerfed current record caches!");
                            log.debug("historicalDatasetEditsByCommitDateAsc now: {}", historicalDatasetEditsByCommitDateAsc);
                            log.debug("historicalPackagePurlsByDatasourcePurl is now: {}", historicalPackagePurlsByDatasourcePurl);
                            log.debug("historicalFindingsByDatasourcePurl is now: {}", historicalFindingsByDatasourcePurl);    
                            log.debug("purlList is: {}", purlList);

                            throw new IllegalStateException();
                        }

                    }
                }

                log.info("historicalDatasetEditsByCommitDateAsc size now: {}", historicalDatasetEditsByCommitDateAsc.size());
                log.info("historicalPackagePurlsByDatasourcePurl size is now: {}", historicalPackagePurlsByDatasourcePurl.size());
                log.info("historicalFindingsByDatasourcePurl size is now: {}", historicalFindingsByDatasourcePurl.size());

                entityManager.clear();

                log.debug("datasetMetricsRecord patches at end is: {}", datasetMetricsRecord.getPatches());

                log.info("***** record processing duration: {}", Duration.between(startProcessingRecord, Instant.now()));
            } catch (Exception e) {
                log.error("caught unexpected exception", e);
                log.error("stack trace is: {}", e.getStackTrace().toString());
                errorRecordIdsAscByDate.add(currentId);

                var datasourceEventRecordOptional = datasourceEventRepository.findById(currentId);
                if (datasourceEventRecordOptional.isEmpty()) {
                    log.error("supplied datasourceEvent record id not found in database: {}", currentId);
                } else {
                    var dse = datasourceEventRecordOptional.get();
                    log.error("setting state PROCESSING_ERROR for datasourceEvent: {}", dse.getPurl());
                    dse.setStatus(DatasourceEvent.Status.PROCESSING_ERROR);
                    //dse = datasourceEventRepository.save(dse);
                    datasourceEventJdbcRepository.updateDatasourceEvent(dse);

                    var ds = dse.getDatasource();
                    log.error("setting state PROCESSING_ERROR for datasource: {}", ds.getPurl());
                    ds.setStatus(Datasource.Status.PROCESSING_ERROR);
                    ds = datasourceRepository.save(ds);
                }

            }

        }

        // default to things went ok 
        var statusCode = HttpStatus.CREATED.value();

        // if there are only error ids something went totally sideways
        if ( !errorRecordIdsAscByDate.isEmpty() && createdRecordIdsAscByDate.isEmpty() ) {
            statusCode = HttpStatus.BAD_REQUEST.value();
        }
        // if both lists are populated some data was processed and some was not  
        else if ( !errorRecordIdsAscByDate.isEmpty() && !createdRecordIdsAscByDate.isEmpty() ) {
            statusCode = HttpStatus.PARTIAL_CONTENT.value();
        }
        
        var data = Map.of(
            "data", (Object)Map.of(
                "createdDatasetMetricsRecordIds", createdRecordIdsAscByDate, 
                "errorDatasourceEventRecordIds", errorRecordIdsAscByDate,
                "allDatasourceEventRecordIds", datasourceEventIndexesByCommitDateAsc,
                "pageIndex", pageIndex,
                "pageSize", pageSize ,
                "datasetName", datasetName
            )
        );

        entityManager.flush();

        // ===== REDIS CACHE INTEGRATION: SAVE CACHES FOR NEXT PAGE =====
        log.info("Saving caches to Redis for dataset: {}, page: {}", datasetName, pageIndex);
        try {
            cacheService.saveCaches(
                datasetName,
                pageIndex,
                historicalPackagePurlsByDatasourcePurl,
                historicalFindingsByDatasourcePurl,
                historicalDatasetEditsByCommitDateAsc
            );
            log.info("Successfully saved caches to Redis");
        } catch (Exception e) {
            log.error("Failed to save caches to Redis (non-fatal, will rebuild on next page)", e);
        }
        // ===== END REDIS CACHE INTEGRATION =====

        return ApiResponse.builder()
                          .txid(txid)
                          .requestReceivedAt(requestReceivedAt)
                          .code(statusCode)
                          .data(data)
                          .build();
    }




    private DatasetMetrics updateCachesAndMetricsRecordWithPackageEdits(
            ConcurrentHashMap<ZonedDateTime, Map<String, Set<Pair<CvssSeverity, String>>>> historicalFindingsByDatasourcePurl,
            ConcurrentHashMap<ZonedDateTime, Map<String, Set<String>>> historicalPackagePurlsByDatasourcePurl,
            DatasourceEvent datasourceEventRecord,
            DatasetMetrics datasetMetricsRecord,
            Map<ZonedDateTime, Set<Pair<String, String>>> historicalDatasetEditsByCommitDateAsc
    ) throws MalformedPackageURLException {

        var datasourceEventPurl = datasourceEventRecord.getPurl();
        var datasourcePurl = new PackageURL(datasourceEventPurl).getCoordinates();
        var currentCommitDateTime = datasourceEventRecord.getCommitDateTime();

        var datasourceEventPackagePurls = datasourceEventRecord.getPackages()
                                                            .stream()
                                                            .map(dse -> dse.getPurl())
                                                            //.collect(Collectors.toSet());
                                                            .collect(Collectors.toList());

        log.info("datasourcePurl is: {}", datasourcePurl);
        
        if (datasourceEventPackagePurls.isEmpty()) {
            log.error("datasourceEventRecord is missing package data!");
            throw new IllegalStateException();
        }

        log.debug("datasourceEventPackagePurls is: {}", datasourceEventPackagePurls);

        // Prepare package families for current packages
        var currentPackageFamilies = datasourceEventRecord.getPackages()
                                                        .stream()
                                                        .map(p -> {
                                                            try {
                                                                return getPurlFamilyString(p.getPurl());
                                                            } catch (MalformedPackageURLException e) {
                                                                log.error("Error getting family for PURL: {}", p.getPurl(), e);
                                                                return p.getPurl(); // fallback
                                                            }
                                                        })
                                                        .collect(Collectors.toList());

        // Convert to comma-delimited strings for stored procedure
        var currentPackagePurlsString = String.join(",", datasourceEventPackagePurls);
        var currentPackageFamiliesString = String.join(",", currentPackageFamilies);

        // Determine historical context
        var historicalCommitDateTime = 
            historicalDatasetEditsByCommitDateAsc.isEmpty() 
                    ? currentCommitDateTime 
                    : new ArrayList<>(historicalDatasetEditsByCommitDateAsc.keySet())  
                                                                .stream()
                                                                .sorted((x1, x2) -> x1.compareTo(x2))
                                                                .toList()
                                                                .getLast();

        var alreadySeenCurrentCommitDateTime = historicalPackagePurlsByDatasourcePurl.containsKey(currentCommitDateTime);
        var historicalAndCurrentCommitDateTimeSame = historicalCommitDateTime.equals(currentCommitDateTime);

        // Prepare historical data strings
        String historicalPackagePurlsString = null;
        String historicalPackageFamiliesString = null;

        if (!alreadySeenCurrentCommitDateTime || !historicalAndCurrentCommitDateTimeSame) {
            var historicalPackagePurlMap = historicalPackagePurlsByDatasourcePurl.get(historicalCommitDateTime);
            if (historicalPackagePurlMap != null && historicalPackagePurlMap.containsKey(datasourcePurl)) {
                var historicalPackagePurls = historicalPackagePurlMap.get(datasourcePurl);
                historicalPackagePurlsString = String.join(",", historicalPackagePurls);
                
                // Generate families for historical packages
                var historicalPackageFamilies = historicalPackagePurls.stream()
                                                                    .map(purl -> {
                                                                        try {
                                                                            return getPurlFamilyString(purl);
                                                                        } catch (MalformedPackageURLException e) {
                                                                            log.error("Error getting family for historical PURL: {}", purl, e);
                                                                            return purl; // fallback
                                                                        }
                                                                    })
                                                                    .collect(Collectors.toList());
                historicalPackageFamiliesString = String.join(",", historicalPackageFamilies);
            }
        }

        log.info("Calling stored procedure with {} current packages", datasourceEventPackagePurls.size());

        // Save the dataset metrics record first to get an ID
        datasetMetricsRecord = datasetMetricsRepository.save(datasetMetricsRecord);

        log.info("saved dsm record: {}", datasetMetricsRecord.getId());

        // Call the stored procedure to detect edits and get statistics
        var editStatistics = jdbcTemplate.queryForObject(
            "SELECT * FROM process_package_edits_and_statistics(?::BIGINT, ?::BIGINT, ?::TIMESTAMP WITH TIME ZONE, ?::TIMESTAMP WITH TIME ZONE, ?::VARCHAR, ?::VARCHAR, ?::VARCHAR, ?::VARCHAR, ?::VARCHAR, ?::BOOLEAN, ?::BOOLEAN)",
            (rs, rowNum) -> new EditStatistics(
                rs.getInt("total_patches"),
                rs.getInt("same_patches"), 
                rs.getInt("different_patches"),
                rs.getInt("pf_patches")
            ),
            datasetMetricsRecord.getId(),
            datasourceEventRecord.getId(),
            currentCommitDateTime.toString(), // Convert to string
            historicalCommitDateTime.toString(), // Convert to string
            datasourcePurl,
            currentPackagePurlsString,
            currentPackageFamiliesString,
            historicalPackagePurlsString, // May be null - cast handled in SQL
            historicalPackageFamiliesString, // May be null - cast handled in SQL
            alreadySeenCurrentCommitDateTime,
            historicalAndCurrentCommitDateTimeSame
        );

        // log.info("done with store procedure. flushing and clearing entity manager");

        // entityManager.flush();
        // entityManager.clear();

        // log.info("entity manager flushed and cleared - now retrieving new dsm record...");
        // datasetMetricsRecord = datasetMetricsRepository.findById(datasetMetricsRecord.getId()).get();
        log.info("done with stored procedure. refreshing entity manager...");
        entityManager.refresh(datasetMetricsRecord);

        log.info("Stored procedure completed. Total patches: {}, Same patches: {}", 
                editStatistics.totalPatches(), editStatistics.samePatches());

        // Update cache with current packages
        if (!alreadySeenCurrentCommitDateTime) {
            historicalPackagePurlsByDatasourcePurl.put(currentCommitDateTime, new ConcurrentHashMap<String, Set<String>>());
            historicalFindingsByDatasourcePurl.put(currentCommitDateTime, new ConcurrentHashMap<String, Set<Pair<CvssSeverity, String>>>());
        }

        // Update package cache
        Map<String, Set<String>> previousDatasourcePurlMap = 
                historicalPackagePurlsByDatasourcePurl.get(historicalCommitDateTime);
            
        var newDatasourcePurlMap = new ConcurrentHashMap<String, Set<String>>();
        for (var es : previousDatasourcePurlMap.entrySet()) {
            newDatasourcePurlMap.put(es.getKey(), new HashSet<>(es.getValue()));
        }
                                                            
        newDatasourcePurlMap.put(datasourceEventRecord.getDatasource().getPurl(), new HashSet<>(datasourceEventPackagePurls));
        historicalPackagePurlsByDatasourcePurl.put(currentCommitDateTime, newDatasourcePurlMap);

        // Update findings cache
        var findings = datasourceEventRecord.getPackages()
                                            .stream()
                                            .map(p -> p.getFindings())
                                            .flatMap(x -> x.stream())
                                            .collect(Collectors.toSet());

        var findingPairs = findings.stream()
                                .map(f -> new Pair<>(CvssSeverity.valueOf(f.getData().getSeverity()), f.getIdentifier()))
                                .collect(Collectors.toSet());

        var previousFindingsMap = 
                historicalFindingsByDatasourcePurl.get(historicalCommitDateTime);

        var newFindingsMap = new ConcurrentHashMap<String, Set<Pair<CvssSeverity, String>>>();
        for (var es : previousFindingsMap.entrySet()) {
            newFindingsMap.put(es.getKey(), new HashSet<>(es.getValue()));
        }

        newFindingsMap.put(datasourcePurl, findingPairs);
        historicalFindingsByDatasourcePurl.put(currentCommitDateTime, newFindingsMap);

        // Update edit cache - fetch the edits that were just created by the stored procedure
        //Set<Pair<String, String>> editPairs = editRepository.getEditPairsByCommitDateTime(currentCommitDateTime);

        Set<Pair<String, String>> editPairs = jdbcTemplate.query(
            "SELECT * FROM get_edit_pairs_for_cache(?::BIGINT, ?::TIMESTAMP WITH TIME ZONE)",
            (rs, rowNum) -> new Pair<>(rs.getString(1), rs.getString(2)),
            datasetMetricsRecord.getId(), 
            currentCommitDateTime.toString()
        ).stream().collect(Collectors.toSet());

        historicalDatasetEditsByCommitDateAsc.put(datasetMetricsRecord.getCommitDateTime(), editPairs);

        // Update dataset metrics with statistics from stored procedure
        datasetMetricsRecord.setPatches(editStatistics.totalPatches());
        datasetMetricsRecord.setDifferentPatches(editStatistics.differentPatches());
        datasetMetricsRecord.setSamePatches(editStatistics.samePatches());
        datasetMetricsRecord.setPatchFoxPatches(editStatistics.pfPatches());
        long dsmId = datasetMetricsRecord.getId();

        //
        // TODO this is here because somehow a downstream stored proc is not respecting the datasetMetrics.save() of
        // the JPA entity and is serializing a "zero" for these values vs what they should be. 
        //

        // Update dataset metrics with statistics from stored procedure
        jdbcTemplate.execute(
            "SELECT update_dataset_metrics_patches(?, ?, ?, ?, ?)",
            (PreparedStatement ps) -> {
                ps.setLong(1, dsmId);
                ps.setLong(2, editStatistics.totalPatches());
                ps.setLong(3, editStatistics.samePatches());
                ps.setLong(4, editStatistics.differentPatches());
                ps.setLong(5, editStatistics.pfPatches());
                return ps.execute();
            }
        );

        //
        // TODO this is getting fucky -- we need to pivot to only updating this record by way of stored proc
        // vs both in jpa entities and stored proc 
        //
        //datasetMetricsRecord = datasetMetricsRepository.save(datasetMetricsRecord);

        // Flush JPA changes and sync with database
        // entityManager.flush();
        // entityManager.clear();
        // // Re-fetch to get stored proc's changes
        // datasetMetricsRecord = datasetMetricsRepository.findById(datasetMetricsRecord.getId()).get();

        entityManager.refresh(datasetMetricsRecord);
        log.debug("datasetMetricsRecord patches is now: {}", datasetMetricsRecord.getPatches());
        return datasetMetricsRecord;
    }

    // Helper record class for edit statistics
    public record EditStatistics(
        int totalPatches,
        int samePatches,
        int differentPatches,
        int pfPatches
    ) {}






    /*
     * 
     * THIS WORKS BUT IS SLOWER AT SCALE THAN IS DESIRED
     * 
     * KEEP IT FOR NOW 
     * 
     * 
     */

    // /**
    //  * 
    //  * @param historicalFindingsByDatasourcePurl
    //  * @param historicalPackagePurlsByDatasourcePurl
    //  * @param datasourceEventRecord
    //  * @param datasetMetricsRecord
    //  * @param historicalDatasetMetricRecordsByEventDateAsc
    //  * @return
    //  * @throws MalformedPackageURLException
    //  */
    // private DatasetMetrics updateCachesAndMetricsRecordWithPackageEdits(
    //         ConcurrentHashMap<ZonedDateTime, Map<String, Set<Pair<CvssSeverity, String>>>> historicalFindingsByDatasourcePurl,
    //         ConcurrentHashMap<ZonedDateTime, Map<String, Set<String>>> historicalPackagePurlsByDatasourcePurl,
    //         DatasourceEvent datasourceEventRecord,
    //         DatasetMetrics datasetMetricsRecord,
    //         Map<ZonedDateTime, Set<Pair<String, String>>> historicalDatasetEditsByCommitDateAsc
    // ) throws MalformedPackageURLException {

    //     var edits = new HashSet<Edit>();

    //     var datasourceEventPurl = datasourceEventRecord.getPurl();

    //     var datasourcePurl = new PackageURL(datasourceEventPurl).getCoordinates();

    //     var datasourceEventPackagePurls = datasourceEventRecord.getPackages()
    //                                                            .stream()
    //                                                            .map(dse -> dse.getPurl())
    //                                                            .collect(Collectors.toSet());

    //     log.info("datasourcePurl is: {}", datasourcePurl);
        
    //     if (datasourceEventPackagePurls.isEmpty()) {
    //         log.error("datasourceEventRecord is missing package data!");
    //         throw new IllegalStateException();
    //     }

    //     log.debug("datasourceEventPackagePurls is: {}", datasourceEventPackagePurls);

    //     var currentCommitDateTime = datasourceEventRecord.getCommitDateTime();
    //     List<String> previousDatasourcePackagePurls = new ArrayList<>();

    //     // creates defensive copy because some async code also touches this and it was causing problems 
    //     var historicalCommitDateTime = 
    //         historicalDatasetEditsByCommitDateAsc.isEmpty() 
    //                 ? currentCommitDateTime 
    //                 : new ArrayList<>(historicalDatasetEditsByCommitDateAsc.keySet())  
    //                                                                        .stream()
    //                                                                        .sorted((x1, x2) -> x1.compareTo(x2))
    //                                                                        .toList()
    //                                                                        .getLast();

    //     // 
    //     // because sometimes more than one datasourceEvent can have the same commitDateTime and commitHash 
    //     // https://patchfox-workspace.slack.com/archives/C067UNTG350/p1741384967810699
    //     // 
    //     var alreadySeenCurrentCommitDateTime = historicalPackagePurlsByDatasourcePurl.containsKey(currentCommitDateTime);
    //     var historicalAndCurrentCommitDateTimeSame = historicalCommitDateTime.equals(currentCommitDateTime);
        
    //     if ( !alreadySeenCurrentCommitDateTime ) {
    //         historicalPackagePurlsByDatasourcePurl.put(currentCommitDateTime, new ConcurrentHashMap<String, Set<String>>());
    //         historicalFindingsByDatasourcePurl.put(currentCommitDateTime, new ConcurrentHashMap<String, Set<Pair<CvssSeverity, String>>>());
    //     } else {
    //         log.info("*** found multiple datasourceEvent records referencing the same commitDateTime ***");
    //     }

    //     log.info("currentCommitDateTime is: {}", currentCommitDateTime);
    //     log.info("historicalCommitDateTime is: {}", historicalCommitDateTime);
    //     log.info("datasourcePurl is: {}", datasourcePurl);
    //     log.debug("historicalPackagePurlsByDatasourcePurl is: {}", historicalPackagePurlsByDatasourcePurl);

    //     // if the commitDatetime is new to us and it's the same as current it's our first cycle through this loop 
    //     // and we can assume everything is new
    //     if ( !alreadySeenCurrentCommitDateTime && historicalAndCurrentCommitDateTimeSame) {
    //         for (var p : datasourceEventRecord.getPackages()) {
    //             var edit = Edit.builder()
    //                            .datasetMetrics(datasetMetricsRecord)
    //                            .before("") 
    //                            .after(p.getPurl())
    //                            .commitDateTime(datasourceEventRecord.getCommitDateTime())
    //                            .eventDateTime(datasourceEventRecord.getEventDateTime())
    //                            .datasource(datasourceEventRecord.getDatasource())
    //                            .editType(EditType.CREATE)
    //                            .isUserEdit(true)
    //                            .criticalFindings(0)
    //                            .highFindings(0)
    //                            .mediumFindings(0)
    //                            .lowFindings(0)
    //                            .build();

    //             edits.add(edit);
    //         }

    //     } 
    //     // if we're here there's some history we need to look through to understand how to treat the new data
    //     else {

    //         // if no key then it's a new datasource and everything is net new 
    //         if ( !historicalPackagePurlsByDatasourcePurl.get(historicalCommitDateTime).containsKey(datasourcePurl) ) {
    //             for (var p : datasourceEventRecord.getPackages()) {
    //                 var edit = Edit.builder()
    //                                .datasetMetrics(datasetMetricsRecord)
    //                                .before("") 
    //                                .after(p.getPurl())
    //                                .commitDateTime(datasourceEventRecord.getCommitDateTime())
    //                                .eventDateTime(datasourceEventRecord.getEventDateTime())
    //                                .datasource(datasourceEventRecord.getDatasource())
    //                                .editType(EditType.CREATE)
    //                                .isUserEdit(true)
    //                                .criticalFindings(0)
    //                                .highFindings(0)
    //                                .mediumFindings(0)
    //                                .lowFindings(0)
    //                                .build();

    //                 edits.add(edit);
    //             }
    //         } else {

    //             previousDatasourcePackagePurls = 
    //                 historicalPackagePurlsByDatasourcePurl.get(historicalCommitDateTime)
    //                                                       .get(datasourcePurl)
    //                                                       .stream()
    //                                                       .toList();

    //             for (var p : datasourceEventRecord.getPackages()) {
    //                 var edit = Edit.builder()
    //                                .datasetMetrics(datasetMetricsRecord)
    //                                .commitDateTime(datasourceEventRecord.getCommitDateTime())
    //                                .eventDateTime(datasourceEventRecord.getEventDateTime())
    //                                .datasource(datasourceEventRecord.getDatasource())
    //                                .before("")
    //                                .after(p.getPurl())
    //                                .isUserEdit(true)
    //                                .criticalFindings(0)
    //                                .highFindings(0)
    //                                .mediumFindings(0)
    //                                .lowFindings(0);

    //                 // not an edit if namespace, name, and version match 
    //                 if (previousDatasourcePackagePurls.contains(p.getPurl())) {
    //                     continue;
    //                 } 
    //                 //
    //                 else {
    //                     var pFamily = getPurlFamilyString(p.getPurl());
    //                     var foundPreviousFamily = false;
    //                     for (int i = 0; i < previousDatasourcePackagePurls.size(); i ++) {
    //                         var ppFamily = getPurlFamilyString(previousDatasourcePackagePurls.get(i));
    //                         if (ppFamily.equals(pFamily)) {
    //                             edit.editType(Edit.EditType.UPDATE);
    //                             edit.before(previousDatasourcePackagePurls.get(i));
    //                             foundPreviousFamily = true;
    //                         }
    //                     }

    //                     if ( !foundPreviousFamily) {
    //                         edit.editType(Edit.EditType.CREATE);
    //                     }

    //                     edits.add(edit.build());
    //                 }
    //             }
    //         }
    //     }


    //     //
    //     // update historicalPackagePurlsByDatasourcePurl cache
    //     //
    //     // in the case of multiple datasourceEvent records with the same commitDateTime, earlier in this method a 
    //     // block was NOT activated that would have put a new collection in the map keyed to commitDateTime (because
    //     // doing so would overwrite the previous record).
    //     //
    //     // what will happen here is the existing cache record will be updated 
    //     //

    //     var currentDatasourceEventPurls = datasourceEventRecord.getPackages()
    //                                                            .stream()
    //                                                            .map(p -> p.getPurl())
    //                                                            .collect(Collectors.toSet());

    //     Map<String, Set<String>> previousDatasourcePurlMap = 
    //             historicalPackagePurlsByDatasourcePurl.get(historicalCommitDateTime);
            
    //     var newDatasourcePurlMap = new ConcurrentHashMap<String, Set<String>>();
    //     for (var es : previousDatasourcePurlMap.entrySet()) {
    //         newDatasourcePurlMap.put(es.getKey(), new HashSet<>(es.getValue()));
    //     }
                                                            
    //     newDatasourcePurlMap.put(datasourceEventRecord.getDatasource().getPurl(), currentDatasourceEventPurls);
    //     historicalPackagePurlsByDatasourcePurl.put(currentCommitDateTime, newDatasourcePurlMap);


    //     var currentPurlSet = historicalPackagePurlsByDatasourcePurl.get(currentCommitDateTime)
    //                                                                .values()
    //                                                                .stream()
    //                                                                .flatMap(Collection::stream)
    //                                                                .collect(Collectors.toSet());

    //     var previousPurlSet = historicalPackagePurlsByDatasourcePurl.get(historicalCommitDateTime)
    //                                                                 .values()
    //                                                                 .stream()
    //                                                                 .flatMap(Collection::stream)
    //                                                                 .collect(Collectors.toSet());

    //     log.debug("currentPurlSet: {}", currentPurlSet);
    //     log.debug("previousPurlSet: {}", previousPurlSet);

    //     previousPurlSet.removeAll(currentPurlSet);
    //     log.debug("previousPurlSet is now: {}", previousPurlSet);



    //     /*
    //      * now do the same thing for the findings cache 
    //      */
    //     var findings = datasourceEventRecord.getPackages()
    //                                         .stream()
    //                                         .map(p -> p.getFindings())
    //                                         .flatMap(x -> x.stream())
    //                                         .collect(Collectors.toSet());

    //     var findingPairs = findings.stream()
    //                                .map(f -> new Pair<>(CvssSeverity.valueOf(f.getData().getSeverity()), f.getIdentifier()))
    //                                .collect(Collectors.toSet());


    //     var previousFindingsMap = 
    //             historicalFindingsByDatasourcePurl.get(historicalCommitDateTime);

    //     var newFindingsMap = new ConcurrentHashMap<String, Set<Pair<CvssSeverity, String>>>();
    //     for (var es : previousFindingsMap.entrySet()) {
    //         newFindingsMap.put(es.getKey(), new HashSet<>(es.getValue()));
    //     }

    //     newFindingsMap.put(datasourcePurl, findingPairs);
    //     historicalFindingsByDatasourcePurl.put(currentCommitDateTime, newFindingsMap);



    //     /*
    //      * 
    //      * suss out the last of the edits 
    //      */

    //     for (var ppAsString : previousPurlSet) {

    //         boolean isDeleteEdit = true;
    //         for (var edit : edits) {
    //             if (edit.getBefore().equals(ppAsString)) { 
    //                 isDeleteEdit = false; 
    //                 break;
    //             }
    //         }

    //         if ( !isDeleteEdit ) { continue; }

    //         var edit = Edit.builder()
    //                        .datasetMetrics(datasetMetricsRecord)
    //                        .before(ppAsString) 
    //                        .after("")
    //                        .editType(EditType.DELETE)
    //                        .commitDateTime(datasourceEventRecord.getCommitDateTime())
    //                        .eventDateTime(datasourceEventRecord.getEventDateTime())
    //                        .datasource(datasourceEventRecord.getDatasource())
    //                        .isUserEdit(true)
    //                        .criticalFindings(0)
    //                        .highFindings(0)
    //                        .mediumFindings(0)
    //                        .lowFindings(0)
    //                        .build();

    //         edits.add(edit);
    //     }



    //     // forgive me father for I have sinned...
    //     var historicalCommitDateTimesAsc = 
    //         historicalDatasetEditsByCommitDateAsc.keySet()
    //                                              .stream()
    //                                              .sorted((x1, x2) -> x1.compareTo(x2))
    //                                              .toList();

    //     for (var commitDateTime : historicalCommitDateTimesAsc) {
    //         var historicalEdits = historicalDatasetEditsByCommitDateAsc.get(commitDateTime);
    //         for(var currentEdit: edits) {
    //             // check for same or patchfox edit
    //             var sameEditCountForPesOnly = 0;
    //             for (var historicalEdit : historicalEdits) {
    //                 if (
    //                     currentEdit.getBefore().equals(historicalEdit.getLeft())
    //                     && currentEdit.getAfter().equals(historicalEdit.getRight())
    //                 ) {
    //                     sameEditCountForPesOnly += 1;
    //                     currentEdit.setSameEdit(true);
    //                     currentEdit.setSameEditCount(sameEditCountForPesOnly);
    //                 }
    //             }
    //         }
    //     }
    
    //     var sameEditCount = edits.stream().filter(x -> x.isSameEdit()).toList().size();
    //     var pfEditCount = edits.stream().filter(x -> x.isPfRecommendedEdit()).toList().size();
    //     var differentPatches = (edits.size() - sameEditCount) < 0 ? 0 : edits.size() - sameEditCount;

 
    //     log.info("************ edits size is: {}", edits.size());
    //     for (var e : edits) {
    //         log.info("EDIT>>>   " + e.getEditType() + " " + e.getBefore() + " " + e.getAfter() + " " + e.getCommitDateTime());
    //     }

    //     var editPairs = edits.stream()
    //                          .map(e -> new Pair<>(e.getBefore(), e.getAfter()))
    //                          .collect(Collectors.toSet());

    //     historicalDatasetEditsByCommitDateAsc.put(
    //         datasetMetricsRecord.getCommitDateTime(), 
    //         editPairs
    //     );

    //     datasetMetricsRecord = datasetMetricsRepository.save(datasetMetricsRecord);
    //     editRepository.saveAll(edits);
    //     datasetMetricsRecord.setEdits(edits);
    //     datasetMetricsRecord.setPatches(edits.size());

    //     // we are assuming the following
    //     // this method is called on a per-datasourceEvent basis and in temporal order. 
    //     // therefore - and unlike the "edits" and "patches" which are a function in part 
    //     // of the previous value, for different, same, and pf patches we only want the last
    //     // value. ie - the one derived from the most recent (last in list) datasourceEvent information
    //     // because we are comparing that to the historical cache. 
    //     // 
    //     // TODO - not a good assumption to make as this makes things brittle. refactor this or yea be damned 
    //     // to the depths of 3am pager duty...
    //     datasetMetricsRecord.setDifferentPatches(differentPatches);
    //     datasetMetricsRecord.setSamePatches(sameEditCount);
    //     datasetMetricsRecord.setPatchFoxPatches(pfEditCount);

    //     //
    //     return datasetMetricsRecord;
    // }



    /**
     * 
     * 
     * 
     * 
     * 
     * 
     *
     * TODO UPDATE THIS METHOD TO TAKE THE PUBLISHED_AT FIELD IN FINDING_DATA INTO ACCOUNT SO WE DON'T TABULATE
     * FINDINGS THAT WERE PUBLISHED AFTER DSE.COMMIT_DATE_TIME
     * 
     * CURRENTLY GRYPE DOESN'T POPULATE THAT FIELD BUT WHEN NVD-SERVICE IS ONLINE IT WILL 
     * 
     * 
     * 
     * 
     * 
     * 
     * 
     * 
     * 
     * 
     * 
     * 
     * 
     * 
     * 
     * 
     * 
     * 
     * 
     * @param datasourceEventRecord
     * @param historicalPackagePurlsByDatasourcePurl
     * @param historicalPackagePurlsByDatasourcePurl
     * @param historicalDatasetMetricsRecords
     * @param datasetMetricsRecord
     * @return
     */
    private DatasetMetrics updateDatasetMetricsRecordWithCve(
            DatasourceEvent datasourceEventRecord,
            ConcurrentHashMap<ZonedDateTime, Map<String, Set<Pair<CvssSeverity, String>>>> historicalFindingsByDatasourcePurl,
            ConcurrentHashMap<ZonedDateTime, Map<String, Set<String>>> historicalPackagePurlsByDatasourcePurl,
            Map<ZonedDateTime, Set<Pair<String, String>>> historicalDatasetEditsByCommitDateAsc,
            DatasetMetrics datasetMetricsRecord,
            Set<String> currentPackagePurlsWithFindings
    ) {

        var start = Instant.now();

        // 
        // handle current finding tallies
        // 

        // var cveTypeCountMap = new HashMap<>(Map.of(
        //     CvssSeverity.CRITICAL, 0,
        //     CvssSeverity.HIGH, 0,
        //     CvssSeverity.MEDIUM, 0,
        //     CvssSeverity.LOW, 0
        // ));

        var cveIdSet = new HashSet<String>();

        // var totalPackages = 0;
        // var packagesWithFindings = 0;
        // var packagesWithCriticalFindings = 0;
        // var packagesWithHighFindings = 0;
        // var packagesWithMediumFindings = 0;
        // var packagesWithLowFindings = 0;
        var currentCommitDateTime = datasetMetricsRecord.getCommitDateTime();
        //var currentEdits = editRepository.findAllByCommitDateTime(currentCommitDateTime);

        var doneGettingEdits = Instant.now();
        log.info("tally current findings get edits duration: {}", Duration.between(start, doneGettingEdits));
        
        var dsePurls = datasourceEventRecord.getPackages().stream().map(x -> x.getPurl()).toList();
        log.debug("dsePurls Size is: {}", dsePurls.size());
        var dsePurlsString = listToSqlArrayString(dsePurls);
        log.debug("dsePurlsString is: {}", dsePurlsString);
        var dsPurl = datasourceEventRecord.getDatasource().getPurl();
        
        // handling this in a combined call now 
        //
        //editRepository.updateEditFindingCounts(dsePurlsString, currentCommitDateTime, dsPurl);
        //
        //
        //log.info("update edit finding counts duration: {}", Duration.between(doneGettingEdits, Instant.now()));

        var currentPurlSet = historicalPackagePurlsByDatasourcePurl.get(currentCommitDateTime)
                                                                   .values()
                                                                   .stream()
                                                                   .flatMap(Collection::stream)
                                                                   .collect(Collectors.toSet());

        // remove anything from the findings cache that is no longer present in the dataset 
        currentPackagePurlsWithFindings.retainAll(currentPurlSet);

        // JAVA IS PASS BY VALUE NOT PASS BY REFERENCE -- THIS REASSIGNS THE LOCAL COPY OF THE VALUE OF THIS COLLECTION
        // INSTEAD OF REASSIGNING THE REFERENCE TO IT 
        // currentPackagePurlsWithFindings = 
        //     currentPackagePurlsWithFindings.stream()
        //                                    .filter(p -> currentPurlSet.contains(p))
        //                                    .collect(Collectors.toSet());

        // var startCurrentPackagePurlsWithFindings = Instant.now();
        // // now add anything that might be net new from the current datasource_event
        // var dsePurlsWithFindings = packageRepository.filterPurlsWithFindings(dsePurlsString);
        // log.info(
        //     "fetch current package purls with findings duration: {}", 
        //     Duration.between(startCurrentPackagePurlsWithFindings, Instant.now())
        // );
        //currentPackagePurlsWithFindings.addAll(dsePurlsWithFindings);
        currentPackagePurlsWithFindings.addAll(datasourceEventRecord.getPackages().stream().filter(x -> !x.getFindings().isEmpty()).map(x -> x.getPurl()).toList());


        log.debug("currentPurlSet is: {}", currentPurlSet);


        var completePurlListWithFindings = 
                        historicalPackagePurlsByDatasourcePurl.get(currentCommitDateTime)
                                                              .values()
                                                              .stream()
                                                              .flatMap(x -> x.stream())
                                                              .filter(x -> currentPackagePurlsWithFindings.contains(x))
                                                              .toList();


        log.info(
            "Processing {} total packages with findings of {} total packages for dataset metrics ID {} to collate curent findings", 
            //currentPackagePurlsWithFindings.size(),
            completePurlListWithFindings.size(),
            //currentPurlSet.size(), 
            historicalPackagePurlsByDatasourcePurl.get(currentCommitDateTime).values().stream().flatMap(x -> x.stream()).toList().size(),
            datasetMetricsRecord.getId()
        );

        //
        // NOW instead of sending all the purls to the function that updates the dsm record every time it can 
        // now assume it's always being given purls that have findings and save some cycles
        //
        //var stringEncodedPurlSet = setToSqlArrayString(currentPurlSet);

        //var stringEncodedPurlSet = setToSqlArrayString(currentPackagePurlsWithFindings);
        var stringEncodedPurlList = listToSqlArrayString(completePurlListWithFindings);
        log.debug("stringEncodedPurlList is: {}", stringEncodedPurlList);
        var startUpdateAndFetch = Instant.now();
        
        // handling this in a combined call now 
        //
        //datasetMetricsRecord = updateAndFetchDatasetMetrics(stringEncodedPurlSet, datasetMetricsRecord.getId());
        //

        // datasetMetricsRecord = updateEditAndUpdateAndFetchDatasetMetrics(
        //     dsePurlsString, 
        //     currentCommitDateTime,
        //     dsPurl, 
        //     //stringEncodedPurlSet, 
        //     stringEncodedPurlList,
        //     datasetMetricsRecord.getId()
        // );




        log.debug("datasetMetricsRecord patches before updateEditAndUpdateAndFetchDatasetMetricsJdbc is: {}", datasetMetricsRecord.getPatches());



        datasetMetricsRecord = updateEditAndUpdateAndFetchDatasetMetricsJdbc(
            dsePurlsString, 
            currentCommitDateTime,
            dsPurl, 
            stringEncodedPurlList,
            datasetMetricsRecord.getId()
        );


        log.info("update and fetch edit and dsm duration: {}", Duration.between(startUpdateAndFetch, Instant.now()));




        log.debug("datasetMetricsRecord patches after updateEditAndUpdateAndFetchDatasetMetricsJdbc is: {}", datasetMetricsRecord.getPatches());





        var doneCurrentFindings = Instant.now();
        log.info("tally current findings duration: {}", Duration.between(start, doneCurrentFindings));
        //
        // handle backlog finding tallies 
        //
        var backlogThirtyMap = new ConcurrentHashMap<>(Map.of(
            CvssSeverity.CRITICAL, 0,
            CvssSeverity.HIGH, 0,
            CvssSeverity.MEDIUM, 0,
            CvssSeverity.LOW, 0
        ));

        var backlogSixtyMap = new ConcurrentHashMap<>(Map.of(
            CvssSeverity.CRITICAL, 0,
            CvssSeverity.HIGH, 0,
            CvssSeverity.MEDIUM, 0,
            CvssSeverity.LOW, 0
        ));

        var backlogNinetyMap = new ConcurrentHashMap<>(Map.of(
            CvssSeverity.CRITICAL, 0,
            CvssSeverity.HIGH, 0,
            CvssSeverity.MEDIUM, 0,
            CvssSeverity.LOW, 0
        ));

        var historicalCveIds = new HashSet<String>();           
        var currentCommitDateTimeMinusOneMonth = datasetMetricsRecord.getCommitDateTime().minusMonths(1);
        var currentCommitDateTimeMinusTwoMonths = datasetMetricsRecord.getCommitDateTime().minusMonths(2);
        var currentCommitDateTimeMinusThreeMonths = datasetMetricsRecord.getCommitDateTime().minusMonths(3);

        var historicalCommitDateTimeAsc = historicalFindingsByDatasourcePurl.keySet()
                                                                            .stream()
                                                                            .sorted((x1, x2) -> x1.compareTo(x2))
                                                                            .toList();

        //var datasourcePurl = datasourceEventRecord.getDatasource().getPurl();

        // var currentFindingData = 
        //     datasourceEventRecord.getPackages()
        //                          .stream()
        //                          .map(p -> p.getPurl())
        //                          .flatMap(purl -> findingDataRepository.findFindingDataForPackagePurl(purl).stream())
        //                          .toList();

        //var currentFindingData = findingDataRepository.findFindingDataForPackagePurls(new HashSet<>(dsePurls));

        /*
         * 
         * TODO this is goober shit but time is short. tl'dr like the rest of this mess of a service it evolved over
         * the source of a year as a series of monkey patches and hot fixes to get it working and handling scale
         * the purpose here is to discover in the asc sorted list of commitdatetimes the first event that meets 
         * the 30/60/90 days or more criteria. given that the list is asc, we will hit one, then end up skipping
         * [n] commitdatetimes until we hit the next one, and so on. 
         * 
         * the better way to do this would be to filter the list of date times so we pull out ones we want. 
         * 
         * that's future dave's problem
         * 
         */
        // // Create separate CVE tracking for each bucket
        // HashSet<String> thirtyDayHistoricalCveIds = new HashSet<>();
        // HashSet<String> sixtyDayHistoricalCveIds = new HashSet<>(); 
        // HashSet<String> ninetyDayHistoricalCveIds = new HashSet<>();

        // for (var historicalCommitDatetime : historicalCommitDateTimeAsc) {
        //     var isBeforeMinusOneMonth = historicalCommitDatetime.isBefore(currentCommitDateTimeMinusOneMonth);
        //     var isBeforeMinusTwoMonths = historicalCommitDatetime.isBefore(currentCommitDateTimeMinusTwoMonths);
        //     var isBeforeMinusThreeMonths = historicalCommitDatetime.isBefore(currentCommitDateTimeMinusThreeMonths);
            
        //     if (isBeforeMinusThreeMonths) {
        //         log.info("entering backlog90 processing");
        //         backlogNinetyMap = 
        //             updateBacklogMap(
        //                 historicalFindingsByDatasourcePurl,
        //                 currentCommitDateTime,
        //                 historicalCommitDatetime, 
        //                 backlogNinetyMap, 
        //                 ninetyDayHistoricalCveIds  // separate Set
        //             );
        //     } 
            
        //     else if (isBeforeMinusTwoMonths) {
        //         log.info("entering backlog60 processing");
        //         backlogSixtyMap = 
        //             updateBacklogMap(
        //                 historicalFindingsByDatasourcePurl,
        //                 currentCommitDateTime,
        //                 historicalCommitDatetime, 
        //                 backlogSixtyMap, 
        //                 sixtyDayHistoricalCveIds  // separate Set
        //             );
        //     }
            
        //     else if (isBeforeMinusOneMonth) {
        //         log.info("entering backlog30 processing");
        //         backlogThirtyMap = 
        //             updateBacklogMap(
        //                 historicalFindingsByDatasourcePurl,
        //                 currentCommitDateTime,
        //                 historicalCommitDatetime, 
        //                 backlogThirtyMap, 
        //                 thirtyDayHistoricalCveIds  // separate Set
        //             );   
        //     }
        // }


        /*
         * 
         * this works but creates an issue where - if a finding is already in a bucket like 90days and it gets 
         * introduced again in a newly added package the finding gets bucketed as 90 day instead of what it should be 
         * 
         */
        // // Get current findings first
        // List<Pair<CvssSeverity, String>> currentFindings = new ArrayList<>();
        // if (historicalFindingsByDatasourcePurl.keySet().contains(currentCommitDateTime)) {
        //     currentFindings = historicalFindingsByDatasourcePurl.get(currentCommitDateTime)
        //                                                         .values()
        //                                                         .stream()
        //                                                         .flatMap(x -> x.stream())
        //                                                         .toList();
        // }

        // log.info("Processing {} current findings for backlog age calculation", currentFindings.size());

        // // For each current CVE, find when it first appeared
        // for (var currentFinding : currentFindings) {
        //     String cveId = currentFinding.getRight();
        //     CvssSeverity severity = currentFinding.getLeft();
            
        //     // Search backwards through historical data to find first appearance
        //     ZonedDateTime firstAppearance = null;
        //     for (var historicalCommitDatetime : historicalCommitDateTimeAsc) {
        //         if (historicalFindingsByDatasourcePurl.containsKey(historicalCommitDatetime)) {

        //             boolean foundInHistorical = 
        //                 historicalFindingsByDatasourcePurl.get(historicalCommitDatetime)
        //                                                   .values()
        //                                                   .stream()
        //                                                   .flatMap(x -> x.stream())
        //                                                   .anyMatch(hf -> hf.getRight().equals(cveId));
                    
        //             if (foundInHistorical) {
        //                 firstAppearance = historicalCommitDatetime;
        //                 break; // Found first appearance, stop looking
        //             }
        //         }
        //     }
            
        //     // If no historical appearance found, it's brand new (< 30 days)
        //     if (firstAppearance == null) {
        //         continue; // Skip - not in any backlog bucket
        //     }
            
        //     // Calculate age and put in appropriate bucket
        //     long daysBetween = ChronoUnit.DAYS.between(firstAppearance, currentCommitDateTime);
            
        //     if (daysBetween >= 90) {
        //         backlogNinetyMap.put(severity, backlogNinetyMap.getOrDefault(severity, 0) + 1);
        //     } else if (daysBetween >= 60) {
        //         backlogSixtyMap.put(severity, backlogSixtyMap.getOrDefault(severity, 0) + 1);
        //     } else if (daysBetween >= 30) {
        //         backlogThirtyMap.put(severity, backlogThirtyMap.getOrDefault(severity, 0) + 1);
        //     }
        //     // else: < 30 days, not in backlog
        // }
   

        /*
         *
         * 
         * 
         * this one should do a better job of account for the condition the one above doesn't
         * 
         * it's not perfect but because our cache is not always perfect we're at least now making the best use of
         * available data 
         * 
         */
        // Get current package-level findings directly
        var currentPackageFindings = new ArrayList<String[]>();

        // Get package PURLs for current commit datetime
        if (historicalPackagePurlsByDatasourcePurl.containsKey(currentCommitDateTime)) {
            var packagePurls = historicalPackagePurlsByDatasourcePurl.get(currentCommitDateTime)
                                                                    .values()
                                                                    .stream()
                                                                    .flatMap(x -> x.stream())
                                                                    .collect(Collectors.toList());
            
        //     if (!packagePurls.isEmpty()) {
        //         // Get detailed findings with package information
        //         var findingArrays = findingRepository.findSeverityAndIdentifierArraysByPurls(packagePurls);
                
        //         for (var findingArray : findingArrays) {
        //             var packageFinding = new String[]{
        //                 String.valueOf(findingArray[2]), // packagePurl
        //                 String.valueOf(findingArray[1]), // cveId  
        //                 String.valueOf(findingArray[0])  // severity
        //             };
        //             currentPackageFindings.add(packageFinding);
        //         }
        //     }
        // }

            if (!packagePurls.isEmpty()) {
                final int CHUNK_SIZE = 40_000; // safe under 65,535

                for (int i = 0; i < packagePurls.size(); i += CHUNK_SIZE) {
                    var chunk = packagePurls.subList(i, Math.min(i + CHUNK_SIZE, packagePurls.size()));

                    var findingArrays = findingRepository.findSeverityAndIdentifierArraysByPurls(chunk);

                    for (var findingArray : findingArrays) {
                        var packageFinding = new String[]{
                            String.valueOf(findingArray[2]), // packagePurl
                            String.valueOf(findingArray[1]), // cveId
                            String.valueOf(findingArray[0])  // severity
                        };
                        currentPackageFindings.add(packageFinding);
                    }
                }
            }        
        }

        log.info("Processing {} current package-level findings for backlog age calculation", currentPackageFindings.size());

        // For each current package-level finding, find when that specific package+CVE combination first appeared
        for (var currentPackageFinding : currentPackageFindings) {
            String packagePurl = currentPackageFinding[0];
            String cveId = currentPackageFinding[1];
            CvssSeverity severity = CvssSeverity.valueOf(currentPackageFinding[2]);
            
            // Search backwards through historical data
            ZonedDateTime firstAppearance = null;
            for (var historicalCommitDatetime : historicalCommitDateTimeAsc) {
                if (historicalFindingsByDatasourcePurl.containsKey(historicalCommitDatetime)) {
                    var datasourceToFindingsMap = historicalFindingsByDatasourcePurl.get(historicalCommitDatetime);
                    
                    boolean foundPackageCveCombo = false;
                    
                    for (var entry : datasourceToFindingsMap.entrySet()) {
                        String datasourceKey = entry.getKey();
                        
                        if ("HISTORICAL".equals(datasourceKey)) {
                            // For HISTORICAL entries, we can only check CVE ID (no package info)
                            foundPackageCveCombo = entry.getValue().stream()
                                .anyMatch(finding -> finding.getRight().equals(cveId));
                        } else {
                            // For real datasource entries, we can check if the package belongs to this datasource
                            // AND if the CVE exists in this datasource's findings
                            if (historicalPackagePurlsByDatasourcePurl.containsKey(historicalCommitDatetime)) {
                                var packagesByDatasource = historicalPackagePurlsByDatasourcePurl.get(historicalCommitDatetime);
                                
                                if (packagesByDatasource.containsKey(datasourceKey) && 
                                    packagesByDatasource.get(datasourceKey).contains(packagePurl)) {
                                    // This package was in this datasource at this time
                                    foundPackageCveCombo = entry.getValue().stream()
                                        .anyMatch(finding -> finding.getRight().equals(cveId));
                                }
                            }
                        }
                        
                        if (foundPackageCveCombo) break;
                    }
                    
                    if (foundPackageCveCombo) {
                        firstAppearance = historicalCommitDatetime;
                        break; // Found first appearance of this package+CVE combo
                    }
                }
            }
            
            // If no historical appearance found, it's brand new (< 30 days)
            if (firstAppearance == null) {
                continue; // Skip - not in any backlog bucket
            }
            
            // Calculate age and put in appropriate bucket
            long daysBetween = ChronoUnit.DAYS.between(firstAppearance, currentCommitDateTime);
            
            if (daysBetween >= 90) {
                var newValue = backlogNinetyMap.getOrDefault(severity, 0) + 1;
                backlogNinetyMap.put(severity, newValue);
            } else if (daysBetween >= 60) {
                var newValue = backlogSixtyMap.getOrDefault(severity, 0) + 1;
                backlogSixtyMap.put(severity, newValue);
            } else if (daysBetween >= 30) {
                var newValue = backlogThirtyMap.getOrDefault(severity, 0) + 1;
                backlogThirtyMap.put(severity, newValue);
            }
            // else: < 30 days, not in backlog
        }



        //

        var totalBacklogThirty = 
            backlogThirtyMap.get(CvssSeverity.CRITICAL) 
            + backlogThirtyMap.get(CvssSeverity.HIGH)
            + backlogThirtyMap.get(CvssSeverity.MEDIUM)
            + backlogThirtyMap.get(CvssSeverity.LOW);

        var totalBacklogSixty = 
            backlogSixtyMap.get(CvssSeverity.CRITICAL) 
            + backlogSixtyMap.get(CvssSeverity.HIGH)
            + backlogSixtyMap.get(CvssSeverity.MEDIUM)
            + backlogSixtyMap.get(CvssSeverity.LOW);

        var totalBacklogNinety = 
            backlogNinetyMap.get(CvssSeverity.CRITICAL) 
            + backlogNinetyMap.get(CvssSeverity.HIGH)
            + backlogNinetyMap.get(CvssSeverity.MEDIUM)
            + backlogNinetyMap.get(CvssSeverity.LOW);


        log.info("backlogThirty is: {}", backlogThirtyMap);
        log.info("backlogSixty is: {}", backlogSixtyMap);
        log.info("backlogNinety is: {}", backlogNinetyMap);

        datasetMetricsRecord.setFindingsInBacklogBetweenThirtyAndSixtyDays(totalBacklogThirty);
        datasetMetricsRecord.setCriticalFindingsInBacklogBetweenThirtyAndSixtyDays(backlogThirtyMap.get(CvssSeverity.CRITICAL));
        datasetMetricsRecord.setHighFindingsInBacklogBetweenThirtyAndSixtyDays(backlogThirtyMap.get(CvssSeverity.HIGH));
        datasetMetricsRecord.setMediumFindingsInBacklogBetweenThirtyAndSixtyDays(backlogThirtyMap.get(CvssSeverity.MEDIUM));
        datasetMetricsRecord.setLowFindingsInBacklogBetweenThirtyAndSixtyDays(backlogThirtyMap.get(CvssSeverity.LOW));

        //

        datasetMetricsRecord.setFindingsInBacklogBetweenSixtyAndNinetyDays(totalBacklogSixty);
        datasetMetricsRecord.setCriticalFindingsInBacklogBetweenSixtyAndNinetyDays(backlogSixtyMap.get(CvssSeverity.CRITICAL));
        datasetMetricsRecord.setHighFindingsInBacklogBetweenSixtyAndNinetyDays(backlogSixtyMap.get(CvssSeverity.HIGH));
        datasetMetricsRecord.setMediumFindingsInBacklogBetweenSixtyAndNinetyDays(backlogSixtyMap.get(CvssSeverity.MEDIUM));
        datasetMetricsRecord.setLowFindingsInBacklogBetweenSixtyAndNinetyDays(backlogSixtyMap.get(CvssSeverity.LOW));

        //

        datasetMetricsRecord.setFindingsInBacklogOverNinetyDays(totalBacklogNinety);
        datasetMetricsRecord.setCriticalFindingsInBacklogOverNinetyDays(backlogNinetyMap.get(CvssSeverity.CRITICAL));
        datasetMetricsRecord.setHighFindingsInBacklogOverNinetyDays(backlogNinetyMap.get(CvssSeverity.HIGH));
        datasetMetricsRecord.setMediumFindingsInBacklogOverNinetyDays(backlogNinetyMap.get(CvssSeverity.MEDIUM));
        datasetMetricsRecord.setLowFindingsInBacklogOverNinetyDays(backlogNinetyMap.get(CvssSeverity.LOW));

        //

        datasetMetricsRecord = datasetMetricsRepository.save(datasetMetricsRecord);

        var doneBacklogFindings = Instant.now();
        log.info("tally backlog findings duration: {}", Duration.between(doneCurrentFindings, doneBacklogFindings));
        log.info("total time updateDatasetMetricsRecordWithCve: {}",  Duration.between(start, doneBacklogFindings));

        return datasetMetricsRecord;
    }



    public ConcurrentHashMap<CvssSeverity, Integer> updateBacklogMap(
        ConcurrentHashMap<ZonedDateTime, Map<String, Set<Pair<CvssSeverity, String>>>> historicalFindingsByDatasourcePurl,
        ZonedDateTime currentCommitDateTime,
        ZonedDateTime historicalCommitDatetime,
        ConcurrentHashMap<CvssSeverity, Integer> backlogMap,
        HashSet<String> allPreviouslyCounted
    ) {

        List<Pair<CvssSeverity, String>> currentFindings = new ArrayList<>();
        List<Pair<CvssSeverity, String>> historicalFindings = new ArrayList<>();

        if (historicalFindingsByDatasourcePurl.keySet().contains(currentCommitDateTime)) {
            currentFindings = historicalFindingsByDatasourcePurl.get(currentCommitDateTime)
                                                                .values()
                                                                .stream()
                                                                .flatMap(x -> x.stream())
                                                                .toList();
        }

        if (historicalFindingsByDatasourcePurl.keySet().contains(historicalCommitDatetime)) {
            historicalFindings = historicalFindingsByDatasourcePurl.get(historicalCommitDatetime)
                                                                .values()
                                                                .stream()
                                                                .flatMap(x -> x.stream())
                                                                .toList();
        }

        log.info("currentCommitDateTime: {}", currentCommitDateTime);
        log.info("historicalCommitDateTime: {}", historicalCommitDatetime);
        log.info("backlog map is: {}", backlogMap);
        log.info("allPreviouslyCounted size: {}", allPreviouslyCounted.size());
        log.info("historicalFindings size: {}", historicalFindings.size());
        log.info("currentFindings size: {}", currentFindings.size());

        for (var hf : historicalFindings) {
            for (var cfd : currentFindings) {
                if (
                    (cfd.getRight().equals(hf.getRight())) &&
                    (!allPreviouslyCounted.contains(hf.getRight()))  // Check against ALL previously counted CVEs
                ) {
                    allPreviouslyCounted.add(hf.getRight());  // Add to master list
                    var s = hf.getLeft();
                    var newValue = backlogMap.containsKey(s) ? backlogMap.get(s) + 1 : 1;
                    backlogMap.put(s, newValue);
                    break; // Found it, no need to keep checking current findings
                }
            }
        }

        log.info("backlogMap now: {}", backlogMap);
        log.info("allPreviouslyCounted size now: {}", allPreviouslyCounted.size());

        return backlogMap;
    }


    // /**
    //  * 
    //  * @param datasourcePurls
    //  * @param currentDatasourceEventRecord
    //  * @param historicalCommitDatetime
    //  * @param backlogMap
    //  * @param historicalCveIds
    //  * @return
    //  */
    // private /*HashSet<String>*/ ConcurrentHashMap<CvssSeverity, Integer> updateBacklogMap(
    //     ConcurrentHashMap<ZonedDateTime, Map<String, Set<Pair<CvssSeverity, String>>>> historicalFindingsByDatasourcePurl,
    //         ZonedDateTime currentCommitDateTime,
    //         ZonedDateTime historicalCommitDatetime,
    //         ConcurrentHashMap<CvssSeverity, Integer> backlogMap,
    //         HashSet<String> historicalCveIds
    // ) {

    //     // Set<Pair<CvssSeverity, String>> currentFindings = new HashSet<>();
    //     // Set<Pair<CvssSeverity, String>> historicalFindings = new HashSet<>();

    //     List<Pair<CvssSeverity, String>> currentFindings = new ArrayList<>();
    //     List<Pair<CvssSeverity, String>> historicalFindings = new ArrayList<>();


    //     if (historicalFindingsByDatasourcePurl.keySet().contains(currentCommitDateTime)) {
    //         currentFindings = historicalFindingsByDatasourcePurl.get(currentCommitDateTime)
    //                                                             .values()
    //                                                             .stream()
    //                                                             .flatMap(x -> x.stream())
    //                                                             .toList();
    //                                                             //.collect(Collectors.toSet());       
    //     }


    //     if (historicalFindingsByDatasourcePurl.keySet().contains(historicalCommitDatetime)) {
    //         historicalFindings = historicalFindingsByDatasourcePurl.get(historicalCommitDatetime)
    //                                                                .values()
    //                                                                .stream()
    //                                                                .flatMap(x -> x.stream())
    //                                                                .toList();
    //                                                                //.collect(Collectors.toSet());       
    //     }


    //     log.info("currentCommitDateTime: {}", currentCommitDateTime);
    //     log.info("historicalCommitDateTime: {}", historicalCommitDatetime);
    //     log.info("backlog map is: {}", backlogMap);
    //     log.info("historicalCveIds size: {}", historicalCveIds.size());
    //     log.info("historicalFindings size: {}", historicalFindings.size());
    //     log.info("currentFindings size: {}", currentFindings.size());

    //     // forgive me father - I can't stop sinning 
    //     for (var hf : historicalFindings) {
    //         for (var cfd : currentFindings) {
    //             if (
    //                 (cfd.getRight().equals(hf.getRight())) &&
    //                     (!historicalCveIds.contains(cfd.getRight()))
    //             ) {
    //                 historicalCveIds.add(cfd.getRight());
    //                 var s = hf.getLeft();
    //                 var newValue = backlogMap.containsKey(s) ? backlogMap.get(s) + 1 : 1;
    //                 backlogMap.put(s, newValue);
    //             }
    //         }

    //     }

    //     log.info("backlogMap now: {}", backlogMap);
    //     log.info("historicalCveIds size now: {}", historicalCveIds.size());

    //     return backlogMap; //historicalCveIds;
    // }


    // TODO break this out into two methods 


    /**
     * 
     * @param historicalFindingsByDatasourcePurl
     * @param historicalPackagePurlsByDatasourcePurl
     * @param datasetMetricsRecord
     * @param previousDatasetMetricsRecordOptional
     * @return
     */
    private DatasetMetrics updateDatasetMetricsRecordWithRpsAndPes(
        // Map<String, Set<Pair<CvssSeverity, String>>> historicalFindingsByDatasourcePurl,
        // Map<String, Set<String>> historicalPackagePurlsByDatasourcePurl,
        //ConcurrentHashMap<ZonedDateTime, Map<String, Set<Pair<CvssSeverity, String>>>> historicalFindingsByDatasourcePurl,
        ConcurrentHashMap<ZonedDateTime, Map<String, Set<String>>> historicalPackagePurlsByDatasourcePurl,
        DatasetMetrics datasetMetricsRecord,
        Optional<DatasetMetrics> previousDatasetMetricsRecordOptional
    ) {

        var start = Instant.now();
        if (previousDatasetMetricsRecordOptional.isEmpty()) { 
            log.info("total duration for updateDatasetMetricsRecordWithRpsAndPes: {}", Duration.between(start, Instant.now()));
            return datasetMetricsRecord; 
        }

        var previousDatasetMetricsRecord = previousDatasetMetricsRecordOptional.get();

        //
        // RPS
        //

        // loop through by commit so we don't spike memory 
        // we want the value because RPS is a ratio of quanity of package types not quantity of packages.
        var packageFamilyMap = new ConcurrentHashMap<String, Set<String>>();

        var currentCommitDateTime = datasetMetricsRecord.getCommitDateTime();
        var datasourcePurlToPackagePurlMap = historicalPackagePurlsByDatasourcePurl.get(currentCommitDateTime);
        //for (var datasourcePurlToPackagePurlMap : historicalPackagePurlsByDatasourcePurl.values()) {
log.debug("current datasourcePurlToPackagePurlMap is: {}", datasourcePurlToPackagePurlMap);
            var purlList = datasourcePurlToPackagePurlMap.values()
                                                         .stream()
                                                         .flatMap(x -> x.stream())
                                                         .toList();
log.debug("purlList is: {}", purlList);
            for (var purl : purlList) {
                var purlNoVersion = purl.split("@")[0]; 
                if ( !packageFamilyMap.containsKey(purlNoVersion) ) { 
                    packageFamilyMap.put(purlNoVersion, new HashSet<String>()); 
                }
                packageFamilyMap.get(purlNoVersion).add(purl);
            }

        //}
        var totalPackageTypeCount = packageFamilyMap.values()
                                                    .stream()
                                                    .map(x -> x.size())
                                                    .reduce(0, (s, e) -> s + e);
log.debug("packageFamily keys are: {}", packageFamilyMap.keySet());
        var packageFamilyCount = packageFamilyMap.keySet().size();
        var redundantPackageTypeCount = totalPackageTypeCount - packageFamilyCount;
        // in case totalPackageTypeCount == packageFamilyCount to prevent NaN later 
        if (redundantPackageTypeCount == 0) { redundantPackageTypeCount+= 0.1; }

        log.info("totalPacakgeTypeCount: {}", totalPackageTypeCount);
        log.info("redundantPackageTypeCount: {}", redundantPackageTypeCount);

        // rps is the percent of a given dataset's number of unique package types that have at least one other package
        // in the same family (ie - a different version of the same thing). 
        var rps =  ((double)redundantPackageTypeCount / (double)totalPackageTypeCount) * 100;
        log.info("setting RPS score to: {}", rps);
        datasetMetricsRecord.setRpsScore(rps);

        datasetMetricsRecord.setPackageFamilies(new HashSet<>(packageFamilyMap.keySet()));


        //
        // PES
        //


        //
        // we use the backlog vs the current delta because an org has no control of what comes out of nvd - they DO have
        // control over how long something stays in the backlog. 
        var criticalBacklogThirtyDelta = datasetMetricsRecord.getCriticalFindingsInBacklogBetweenThirtyAndSixtyDays() - 
            previousDatasetMetricsRecord.getCriticalFindingsInBacklogBetweenThirtyAndSixtyDays();

        var criticalBacklogSixtyDelta = datasetMetricsRecord.getCriticalFindingsInBacklogBetweenSixtyAndNinetyDays() - 
            previousDatasetMetricsRecord.getCriticalFindingsInBacklogBetweenSixtyAndNinetyDays();

        var criticalBacklogNinetyDelta = datasetMetricsRecord.getCriticalFindingsInBacklogOverNinetyDays() - 
            previousDatasetMetricsRecord.getCriticalFindingsInBacklogOverNinetyDays();     
            
        var highBacklogThirtyDelta = datasetMetricsRecord.getHighFindingsInBacklogBetweenThirtyAndSixtyDays() - 
            previousDatasetMetricsRecord.getHighFindingsInBacklogBetweenThirtyAndSixtyDays();

        var highBacklogSixtyDelta = datasetMetricsRecord.getHighFindingsInBacklogBetweenSixtyAndNinetyDays() - 
            previousDatasetMetricsRecord.getHighFindingsInBacklogBetweenSixtyAndNinetyDays();

        var highBacklogNinetyDelta = datasetMetricsRecord.getHighFindingsInBacklogOverNinetyDays() - 
            previousDatasetMetricsRecord.getHighFindingsInBacklogOverNinetyDays(); 


        var rpsDelta = datasetMetricsRecord.getRpsScore() - previousDatasetMetricsRecord.getRpsScore();

        // effort defaults to number of patches but can be lowered if patches are same patches
        var effort = (double)datasetMetricsRecord.getPatches();


        // impact is a weighted score of the number of good things that happened as a result of this set of edits.
        var impact = 
            (criticalBacklogThirtyDelta * 4)
            + (criticalBacklogSixtyDelta * 5)
            + (criticalBacklogNinetyDelta * 6)
            + (highBacklogThirtyDelta)
            + (highBacklogSixtyDelta * 2)
            + (highBacklogNinetyDelta * 3)
            + rpsDelta;


        // we flip the sign because good deltas are numerically negative and we want impact score to be positive
        // when good shit happens and negative when bad shit happens (ie - vuln or rps numbers go up from previous)
        impact *= -1;
        var edits = editRepository.findAllByCommitDateTime(datasetMetricsRecord.getCommitDateTime());
        for (var edit : edits) { //datasetMetricsRecord.getEdits()) {
            assert (edit.isUserEdit()); // just in case...
            if (edit.isPfRecommendedEdit()) { impact += 1; }
            if (edit.isSameEdit()) {
                var sameEditCount = edit.getSameEditCount();
                // each "edit" is a single action that we assign a "cost" of "1" to. If we've done this in the past 
                // some number of times we can lower the cost of this individual action to account for that. 
                if (sameEditCount > 4) { effort -= 0.75; }
                else if (sameEditCount > 2) { effort -= 0.5; }
            }
        }

        log.info("impact: {}  effort: {}", impact, effort);
        var pes = 0.0;
        if (impact == 0) {
            pes = effort * (-1);
        } else if (effort == 0) {
            pes = impact;
        } else {
            pes = (double)impact / (double)effort;
        }
        datasetMetricsRecord.setPatchImpact(impact);
        datasetMetricsRecord.setPatchEffort(effort);
        datasetMetricsRecord.setPatchEfficacyScore(pes);
        
        log.info("total duration for updateDatasetMetricsRecordWithRpsAndPes: {}", Duration.between(start, Instant.now()));
        return datasetMetricsRecord;
    }



    // /**
    //  * 
    //  * @param commitDateTime
    //  * @param datasourcePurls
    //  * @return
    //  */
    // private List<Finding> getDatasetFindingsForCommitDateTime(        
    //     ZonedDateTime commitDateTime,
    //     List<String> datasourcePurls
    // ) {
    //     var rv = new ArrayList<Finding>();

    //     var datasetPackageSet = getDatasetPackageSetForCommitDateTime(commitDateTime, datasourcePurls);
    //     for (var es : datasetPackageSet.entrySet()) {
    //         var purlList = es.getValue().stream().toList();
    //         var packages = tabulateServiceTransactionalHelpers.getPackagesByPurlIn(purlList);
    //         for (var p : packages) {
    //             rv.addAll(p.getFindings());
    //         }
    //     }

    //     return rv;
    // }



    /**
     * 
     * @param commitDateTime
     * @param datasourcePurls
     * @return
     */
    private Map<String, Set<Pair<CvssSeverity, String>>> 
            getDatasetFindingPairsByDatasourcePurlForCommitDateTime(
                Map<String, Set<String>> datasetPackageSet,
                Set<String> currentPackagePurlsWithFindings
            ) {

        var rv = new ConcurrentHashMap<String, Set<Pair<CvssSeverity, String>>>();

        var futures = new ArrayList<Future<?>>();
        for (var es : datasetPackageSet.entrySet()) {
            Runnable r = () -> { 
                if ( !rv.containsKey(es.getKey()) ) {
                    rv.put(es.getKey(), new HashSet<Pair<CvssSeverity, String>>());
                }
                var purlList = es.getValue().stream().toList();
                rv.get(es.getKey()).addAll(tabulateServiceTransactionalHelpers.findSeverityAndIdentifierByPackagePurlIdsTransactional(purlList, currentPackagePurlsWithFindings));
            };
            futures.add(executorService.submit(r));
        }

        // all of this nonsense to block execution until async tasks are complete ... 
        futures.stream().map(f -> {
            try {
                return f.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error("caught exception while building up list of findings", e);
            }
            return null;
        }).toList();
        
        log.debug("dataset package set is: {}", rv);
        return rv;
    }




    
    private Map<String, Set<String>> getDatasetPackageSetForDatasources(List<String> datasourcePurls) {
        var rv = new ConcurrentHashMap<String, Set<String>>();

        var futures = new ArrayList<Future<?>>();
        for (var datasourcePurl : datasourcePurls) {
            Runnable r = () -> { 
                var packages = tabulateServiceTransactionalHelpers.findPackagePurlsByDatasourcePurlTransactional(datasourcePurl);
                log.debug("datasourcePurl: {}  packages: {}", datasourcePurl, packages);
                rv.put(datasourcePurl, packages); //Collections.unmodifiableSet(packages));
            };
            futures.add(executorService.submit(r));
        }

        // all of this nonsense to block execution until async tasks are complete ... 
        futures.stream().map(f -> {
            try {
                return f.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error("caught exception while building up list of packages", e);
            }
            return null;
        }).toList();
        
        log.debug("dataset package set is: {}", rv);
        return rv;
    }




    // /**
    //  * 
    //  * @param commitDateTime
    //  * @return
    //  */
    // private HashMap<String, Set<String>> getDatasetPackageSetForCommitDateTime(
    //     ZonedDateTime commitDateTime,
    //     List<String> datasourcePurls
    // ) {
    //     var rv = new HashMap<String, Set<String>>();

    //     // var edits = 
    //     //     tabulateServiceTransactionalHelpers.getAllByCommitDateTimeAndDatasourcePurlOnOrBeforeAsc(
    //     //         commitDateTime, 
    //     //         datasourcePurls
    //     //     );
    //     // log.info("edits size is: {}", edits.size());
    //     // for (var edit : edits) { rv = updateDatasetPackageSet(rv, edit); }

    //     var futures = new ArrayList<Future<?>>();
    //     for (var datasourcePurl : datasourcePurls) {
    //         var edits = 
    //             tabulateServiceTransactionalHelpers.getAllByCommitDateTimeAndDatasourcePurlOnOrBeforeAsc(
    //                 commitDateTime, 
    //                 List.of(datasourcePurl)
    //             );

    //         Runnable r = () -> { for (var edit : edits) { updateDatasetPackageSet(rv, edit); } };
    //         futures.add(executorService.submit(r));
    //     }

    //     // all of this nonsense to block execution until async tasks are complete ... 
    //     futures.stream().map(f -> {
    //         try {
    //             return f.get();
    //         } catch (InterruptedException | ExecutionException e) {
    //             log.error("caught exception while building up list of packages", e);
    //         }
    //         return null;
    //     }).toList();
        
    //     log.debug("dataset package set is: {}", rv);
    //     return rv;
    // }


    /**
    //  * 
    //  * @param datasetPackageSet
    //  * @param edit
    //  * @return
    //  */
    // private /*HashMap<String, Set<String>>*/ void updateDatasetPackageSet(
    //     HashMap<String, Set<String>> packageIndexesByDatasourcePurl, 
    //     Edit edit
    // ) {
    //     var datasourcePurl = edit.getDatasource().getPurl();
    //     if ( !packageIndexesByDatasourcePurl.containsKey(datasourcePurl) ) {
    //         log.warn(
    //             "datasource purl: {} not in packageIndexesByDatasourcePurl. Adding empty Set<String>... ", 
    //             datasourcePurl
    //         );
    //         packageIndexesByDatasourcePurl.put(
    //             datasourcePurl, 
    //             new HashSet<String>(new HashSet<String>())
    //         );            
    //     }

    //     switch (edit.getEditType()) {
    //         case Edit.EditType.CREATE:
    //             packageIndexesByDatasourcePurl.get(datasourcePurl).add(edit.getAfter());
    //             break;
    //         case Edit.EditType.UPDATE:
    //             packageIndexesByDatasourcePurl.get(datasourcePurl).remove(edit.getBefore());
    //             packageIndexesByDatasourcePurl.get(datasourcePurl).add(edit.getAfter());
    //             break;
    //         case Edit.EditType.DELETE:
    //             packageIndexesByDatasourcePurl.get(datasourcePurl).remove(edit.getBefore());
    //             break;
    //         default:
    //             log.warn("unexpected edit type: {} -- ignoring...", edit.getEditType());
    //             break;
    //     }

    //     //return packageIndexesByDatasourcePurl;
    // }

    private String encodePath(final String path) {
        return Arrays.stream(path.split("/")).map(segment -> percentEncode(segment)).collect(Collectors.joining("/"));
    }

        /**
     * Encodes the input in conformance with RFC 3986.
     *
     * @param input the String to encode
     * @return an encoded String
     */
    private String percentEncode(final String input) {
        return uriEncode(input, StandardCharsets.UTF_8);
    }

        private static String uriEncode(String source, Charset charset) {
        if (source == null || source.length() == 0) {
            return source;
        }

        StringBuilder builder = new StringBuilder();
        for (byte b : source.getBytes(charset)) {
            if (isUnreserved(b)) {
                builder.append((char) b);
            }
            else {
                // Substitution: A '%' followed by the hexadecimal representation of the ASCII value of the replaced character
                builder.append('%');
                builder.append(Integer.toHexString(b).toUpperCase());
            }
        }
        return builder.toString();
    }

    private static boolean isUnreserved(int c) {
        return (isAlpha(c) || isDigit(c) || '-' == c || '.' == c || '_' == c || '~' == c);
    }

    private static boolean isAlpha(int c) {
        return ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'));
    }

    private static boolean isDigit(int c) {
        return (c >= '0' && c <= '9');
    }


    private String getPurlFamilyString(String purlString) throws MalformedPackageURLException {
        var p = new PackageURL(purlString);
        StringBuilder pFamily = new StringBuilder();
        pFamily.append(p.getScheme()).append(":");
        if (p.getType() != null) {
            pFamily.append(p.getType());
        }
        pFamily.append("/");
        if (p.getNamespace() != null) {
            pFamily.append(encodePath(p.getNamespace()));
            pFamily.append("/");
        }
        if (p.getName() != null) {
            pFamily.append(percentEncode(p.getName()));
        }

        return pFamily.toString();
    }


    /**
     * 
     */
    public String setToSqlArrayString(Set<String> s) {
        return s.toString()
                .replace("[", "")
                .replace("]", "")
                .replace(" ", "");
    }


    /**
     * 
     */
    public String listToSqlArrayString(List<String> l) {
        return l.toString()
                .replace("[", "")
                .replace("]", "")
                .replace(" ", "");
    }

    public String listLongToSqlArrayString(List<Long> l) {
        return l.toString()
                .replace("[", "")
                .replace("]", "")
                .replace(" ", "");
    }


    // @Transactional
    // public DatasetMetrics updateAndFetchDatasetMetrics(String purlsString, Long datasetMetricsId) {
    //     datasetMetricsRepository.updateDatasetMetricsFindings(purlsString, datasetMetricsId);
    //     entityManager.clear(); // to ensure we get the db updated copy - not something the em cached 
    //     return datasetMetricsRepository.findById(datasetMetricsId).orElse(null);
    // }




    // public DatasetMetrics updateEditAndUpdateAndFetchDatasetMetrics(
    //     String editPurlsString, 
    //     ZonedDateTime commitDateTime, 
    //     String datasourcePurl, 
    //     String purlsString, 
    //     Long datasetMetricsId
    // ) {
    //     var startProc = Instant.now();
    //     datasetMetricsRepository.updateEditAndDatasetMetricsFindings(
    //         editPurlsString, 
    //         commitDateTime, 
    //         datasourcePurl, 
    //         purlsString, 
    //         datasetMetricsId
    //     );
    //     log.info("time to enter/exit proc: {}", Duration.between(startProc, Instant.now()));

    //     var entityStart = Instant.now();
    //     entityManager.clear(); // to ensure we get the db updated copy - not something the em cached 
    //     log.info("time to clear entityManager: {}", Duration.between(entityStart, Instant.now()));

    //     var fetchStart = Instant.now();
    //     var datasetMetricsRecord = datasetMetricsRepository.findById(datasetMetricsId).orElse(null);
    //     log.info("time to fetch new dsm: {}", Duration.between(fetchStart, Instant.now()));

    //     return datasetMetricsRecord;
    // }



    public DatasetMetrics updateEditAndUpdateAndFetchDatasetMetrics(
        String editPurlsString, 
        ZonedDateTime commitDateTime, 
        String datasourcePurl, 
        String purlsString, 
        Long datasetMetricsId
    ) {
        log.info("=== HIBERNATE CALL TIMING START ===");
        log.info("Parameter sizes - editPurls: {} chars, datasetPurls: {} chars", 
                editPurlsString.length(), purlsString.length());
        
        var hibernateCallStart = Instant.now();
        var startProc = Instant.now();
        datasetMetricsRepository.updateEditAndDatasetMetricsFindings(
            editPurlsString, 
            commitDateTime, 
            datasourcePurl, 
            purlsString, 
            datasetMetricsId
        );
        var hibernateCallEnd = Instant.now();
        log.info("HIBERNATE CALL TOTAL: {}ms", Duration.between(hibernateCallStart, hibernateCallEnd).toMillis());
        log.info("time to enter/exit proc: {}", Duration.between(startProc, Instant.now()));
        
        var entityStart = Instant.now();
        entityManager.clear(); // to ensure we get the db updated copy - not something the em cached 
        log.info("time to clear entityManager: {}", Duration.between(entityStart, Instant.now()));
        
        var fetchStart = Instant.now();
        var datasetMetricsRecord = datasetMetricsRepository.findById(datasetMetricsId).orElse(null);
        log.info("time to fetch new dsm: {}", Duration.between(fetchStart, Instant.now()));
        
        log.info("=== HIBERNATE CALL TIMING END ===");
        return datasetMetricsRecord;
    }


    public DatasetMetrics updateEditAndUpdateAndFetchDatasetMetricsJdbc(
        String editPurlsString, 
        ZonedDateTime commitDateTime, 
        String datasourcePurl, 
        String purlsString, 
        Long datasetMetricsId
    ) {
        log.info("=== JDBC TEMPLATE CALL TIMING START ===");
        log.info("Parameter sizes - editPurls: {} chars, datasetPurls: {} chars", 
                editPurlsString.length(), purlsString.length());
        
        var jdbcStart = Instant.now();
        jdbcTemplate.execute(
            "SELECT update_edit_and_dataset_metrics_findings(?, ?::timestamptz, ?, ?, ?)",
            (PreparedStatement ps) -> {
                ps.setString(1, editPurlsString);
                ps.setString(2, commitDateTime.toString());
                ps.setString(3, datasourcePurl);
                ps.setString(4, purlsString);
                ps.setLong(5, datasetMetricsId);
                return ps.execute();
            }
        );
        var jdbcEnd = Instant.now();
        log.info("JDBC TEMPLATE CALL TOTAL: {}ms", Duration.between(jdbcStart, jdbcEnd).toMillis());
        
        var entityStart = Instant.now();
        entityManager.clear();
        log.info("time to clear entityManager: {}", Duration.between(entityStart, Instant.now()));
        
        var fetchStart = Instant.now();
        var datasetMetricsRecord = datasetMetricsRepository.findById(datasetMetricsId).orElse(null);
        log.info("time to fetch new dsm: {}", Duration.between(fetchStart, Instant.now()));
        
        log.info("=== JDBC TEMPLATE CALL TIMING END ===");
        return datasetMetricsRecord;
    }


    /*
     * 
     * 
     * THIS CREATES HIBERNATE IMMUTABLE COLLECTION BULLSHIT. SOMEHWERE i tHINK HIBERNATE IS REPLACING COLLECTIONS
     * WE'RE MAKING WITH ITS OWN IMMUTABLE ONES 
     * 
     */

    // /**
    //  * Load DatasourceEvent with packages via single JDBC query preserving timezones
    //  */
    // public DatasourceEvent createDatasourceEventFromJdbcSingleQuery(long id) {
        
    //     // Use custom RowMapper to properly extract timezone info and datasource data
    //     List<Map<String, Object>> results = jdbcTemplate.query(
    //         """
    //         SELECT 
    //             de.id, de.purl, de.txid, de.job_id, de.commit_hash, de.commit_branch,
    //             de.commit_date_time, de.event_date_time, de.status, de.processing_error,
    //             de.oss_enriched, de.package_index_enriched, de.analyzed, de.forecasted, de.recommended,
    //             de.datasource_id,
    //             ds.purl as datasource_purl, ds.name as datasource_name, ds.type as datasource_type,
    //             p.id as package_id, p.purl as package_purl, p.updated_at as package_updated_at
    //         FROM datasource_event de 
    //         LEFT JOIN datasource ds ON de.datasource_id = ds.id
    //         LEFT JOIN datasource_event_package dep ON de.id = dep.datasource_event_id 
    //         LEFT JOIN package p ON dep.package_id = p.id
    //         WHERE de.id = ?
    //         """,
    //         (rs, rowNum) -> {
    //             Map<String, Object> row = new HashMap<>();
    //             row.put("id", rs.getLong("id"));
    //             row.put("purl", rs.getString("purl"));
    //             row.put("txid", rs.getString("txid"));
    //             row.put("job_id", rs.getObject("job_id"));
    //             row.put("commit_hash", rs.getString("commit_hash"));
    //             row.put("commit_branch", rs.getString("commit_branch"));
                
    //             // Extract TIMESTAMP WITH TIME ZONE preserving timezone
    //             row.put("commit_date_time", rs.getObject("commit_date_time", OffsetDateTime.class));
    //             row.put("event_date_time", rs.getObject("event_date_time", OffsetDateTime.class));
                
    //             row.put("status", rs.getString("status"));
    //             row.put("processing_error", rs.getString("processing_error"));
    //             row.put("oss_enriched", rs.getBoolean("oss_enriched"));
    //             row.put("package_index_enriched", rs.getBoolean("package_index_enriched"));
    //             row.put("analyzed", rs.getBoolean("analyzed"));
    //             row.put("forecasted", rs.getBoolean("forecasted"));
    //             row.put("recommended", rs.getBoolean("recommended"));
    //             row.put("datasource_id", rs.getLong("datasource_id"));
                
    //             // Datasource fields
    //             row.put("datasource_purl", rs.getString("datasource_purl"));
    //             row.put("datasource_name", rs.getString("datasource_name"));
    //             row.put("datasource_type", rs.getString("datasource_type"));
                
    //             // Package fields
    //             row.put("package_id", rs.getObject("package_id"));
    //             row.put("package_purl", rs.getString("package_purl"));
                
    //             // Handle package updated_at timestamp with timezone
    //             row.put("package_updated_at", rs.getObject("package_updated_at", OffsetDateTime.class));
                
    //             return row;
    //         },
    //         id
    //     );
        
    //     if (results.isEmpty()) {
    //         throw new RuntimeException("DatasourceEvent not found: " + id);
    //     }
        
    //     // Build the DatasourceEvent from the first row (all rows have same event data)
    //     var firstRow = results.get(0);
    //     var datasourceEvent = new DatasourceEvent();
        
    //     // Set basic fields
    //     datasourceEvent.setId(((Number) firstRow.get("id")).longValue());
    //     datasourceEvent.setPurl((String) firstRow.get("purl"));
    //     datasourceEvent.setTxid(UUID.fromString((String) firstRow.get("txid")));
        
    //     // Handle nullable UUID for jobId
    //     Object jobIdObj = firstRow.get("job_id");
    //     if (jobIdObj != null) {
    //         datasourceEvent.setJobId(UUID.fromString(jobIdObj.toString()));
    //     }
        
    //     // Set string fields
    //     datasourceEvent.setCommitHash((String) firstRow.get("commit_hash"));
    //     datasourceEvent.setCommitBranch((String) firstRow.get("commit_branch"));
    //     datasourceEvent.setProcessingError((String) firstRow.get("processing_error"));
        
    //     // Handle timestamps with preserved timezone info
    //     OffsetDateTime commitTime = (OffsetDateTime) firstRow.get("commit_date_time");
    //     if (commitTime != null) {
    //         // Convert OffsetDateTime to ZonedDateTime preserving exact timezone
    //         datasourceEvent.setCommitDateTime(commitTime.toZonedDateTime());
    //     }
        
    //     OffsetDateTime eventTime = (OffsetDateTime) firstRow.get("event_date_time");
    //     if (eventTime != null) {
    //         // Convert OffsetDateTime to ZonedDateTime preserving exact timezone
    //         datasourceEvent.setEventDateTime(eventTime.toZonedDateTime());
    //     }
        
    //     // Set enum status
    //     String statusStr = (String) firstRow.get("status");
    //     if (statusStr != null) {
    //         datasourceEvent.setStatus(DatasourceEvent.Status.valueOf(statusStr));
    //     }
        
    //     // Set boolean flags
    //     datasourceEvent.setOssEnriched((Boolean) firstRow.get("oss_enriched"));
    //     datasourceEvent.setPackageIndexEnriched((Boolean) firstRow.get("package_index_enriched"));
    //     datasourceEvent.setAnalyzed((Boolean) firstRow.get("analyzed"));
    //     datasourceEvent.setForecasted((Boolean) firstRow.get("forecasted"));
    //     datasourceEvent.setRecommended((Boolean) firstRow.get("recommended"));
        
    //     // Create and set the Datasource entity
    //     var datasource = new Datasource();
    //     datasource.setId((Long) firstRow.get("datasource_id"));
    //     datasource.setPurl((String) firstRow.get("datasource_purl"));
    //     datasource.setName((String) firstRow.get("datasource_name"));
    //     datasource.setType((String) firstRow.get("datasource_type"));
    //     datasourceEvent.setDatasource(datasource);
        
    //     // Build Package entities from all rows that have package data
    //     Set<Package> packages = results.stream()
    //         .filter(row -> row.get("package_id") != null)
    //         .map(row -> {
    //             var pkg = new Package();
                
    //             // Set package ID
    //             pkg.setId(((Number) row.get("package_id")).longValue());
                
    //             // Set PURL (this automatically sets type, namespace, name, version via setPurl method)
    //             String packagePurl = (String) row.get("package_purl");
    //             if (packagePurl != null) {
    //                 pkg.setPurl(packagePurl);
    //             }
                
    //             // Set updated timestamp preserving timezone
    //             OffsetDateTime updatedAt = (OffsetDateTime) row.get("package_updated_at");
    //             if (updatedAt != null) {
    //                 pkg.setUpdatedAt(updatedAt.toZonedDateTime());
    //             }
                
    //             // Initialize empty collections to prevent lazy loading issues
    //             pkg.setFindings(new HashSet<>());
    //             pkg.setCriticalFindings(new HashSet<>());
    //             pkg.setHighFindings(new HashSet<>());
    //             pkg.setMediumFindings(new HashSet<>());
    //             pkg.setLowFindings(new HashSet<>());
    //             pkg.setDatasourceEvents(new HashSet<>());
                
    //             return pkg;
    //         })
    //         .collect(Collectors.toSet());
        
    //     // Set packages on the datasource event
    //     datasourceEvent.setPackages(packages);
        
    //     log.info("Loaded DatasourceEvent {} with {} packages via JDBC", id, packages.size());
        
    //     return datasourceEvent;
    // }



    /**
     * Create a datasource metrics record using the stored procedure
     */
    Long createDatasourceMetricsRecord(
        Long datasourceEventRecordId,
        Long datasetMetricsRecordId,
        Optional<Long> previousDatasetMetricsRecordId,
        Optional<Long> previousDatasourceMetricsRecordId
    ) {        
        // Get previous datasource metrics ID for this datasource (if exists)
        Long previousDatasourceMetricsId = previousDatasourceMetricsRecordId.orElse(null);
        
        // Get previous dataset metrics ID (if exists)
        Long previousDatasetMetricsId = previousDatasetMetricsRecordId.orElse(null);
        
        log.info(
            "Creating datasource metrics record - " +
                "datasourceEvent: {}, " +
                "previousDatasourceMetrics: {}, " +
                "currentDatasetMetrics: {}, " +
                "previousDatasetMetrics: {}", 
            datasourceEventRecordId, 
            previousDatasourceMetricsId, 
            datasetMetricsRecordId, 
            previousDatasetMetricsId
        );
        
        return jdbcTemplate.queryForObject(
            "SELECT create_datasource_metrics_record(?, ?, ?, ?)",
            Long.class,
            datasourceEventRecordId,
            previousDatasourceMetricsId,
            datasetMetricsRecordId,
            previousDatasetMetricsId
        );
    }


    void createDatasourceMetricsCurrentRecord(Long datasourceMetricsRecordId) {
        log.info(
            "Updating datasource_metrics_current record for datasourceMetrics ID: {}", 
            datasourceMetricsRecordId
        );
        
        jdbcTemplate.execute(
            "SELECT update_datasource_metrics_current(?)",
            (PreparedStatement ps) -> {
                ps.setLong(1, datasourceMetricsRecordId);
                ps.execute();
                return null;
            }
        );

        // Flush JPA changes and sync with database
        entityManager.flush();
        entityManager.clear();
    }

}

