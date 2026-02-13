package io.patchfox.analyze_service.services;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import io.patchfox.package_utils.util.CvssSeverity;
import io.patchfox.package_utils.util.Pair;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class CacheService {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    private static final String PACKAGE_CACHE_KEY = "tabulate:packages:";
    private static final String FINDING_CACHE_KEY = "tabulate:findings:";
    private static final String EDIT_CACHE_KEY = "tabulate:edits:";
    private static final String PURLS_WITH_FINDINGS_CACHE_KEY = "tabulate:purlsWithFindings:";
    private static final String BACKLOG_FIRST_APPEARANCE_CACHE_KEY = "tabulate:backlogFirstAppearance:";

    public void saveCaches(
        String datasetName,
        Integer pageIndex,
        ConcurrentHashMap<ZonedDateTime, Map<String, Set<String>>> packageCache,
        ConcurrentHashMap<ZonedDateTime, Map<String, Set<Pair<CvssSeverity, String>>>> findingCache,
        Map<ZonedDateTime, Set<Pair<String, String>>> editCache,
        Set<String> purlsWithFindingsCache,
        Map<String, ZonedDateTime> backlogFirstAppearanceCache
    ) {
        String key = datasetName + ":" + pageIndex;
        
        log.info("Saving caches to Redis for dataset: {}, page: {}", datasetName, pageIndex);
        
        // Delete previous page's cache to avoid accumulation
        if (pageIndex > 0) {
            String prevKey = datasetName + ":" + (pageIndex - 1);
            redisTemplate.delete(PACKAGE_CACHE_KEY + prevKey);
            redisTemplate.delete(FINDING_CACHE_KEY + prevKey);
            redisTemplate.delete(EDIT_CACHE_KEY + prevKey);
            redisTemplate.delete(PURLS_WITH_FINDINGS_CACHE_KEY + prevKey);
            redisTemplate.delete(BACKLOG_FIRST_APPEARANCE_CACHE_KEY + prevKey);
            log.info("Deleted previous cache for page: {}", pageIndex - 1);
        }
        
        redisTemplate.opsForHash().put(PACKAGE_CACHE_KEY + key, "data", packageCache);
        redisTemplate.opsForHash().put(FINDING_CACHE_KEY + key, "data", findingCache);
        redisTemplate.opsForHash().put(EDIT_CACHE_KEY + key, "data", editCache);
        redisTemplate.opsForHash().put(PURLS_WITH_FINDINGS_CACHE_KEY + key, "data", purlsWithFindingsCache);
        redisTemplate.opsForHash().put(BACKLOG_FIRST_APPEARANCE_CACHE_KEY + key, "data", backlogFirstAppearanceCache);
        
        log.info("Caches saved to Redis");
    }

    @SuppressWarnings("unchecked")
    public ConcurrentHashMap<ZonedDateTime, Map<String, Set<String>>> loadPackageCache(
        String datasetName,
        Integer pageIndex
    ) {
        String key = datasetName + ":" + pageIndex;
        Object cached = redisTemplate.opsForHash().get(PACKAGE_CACHE_KEY + key, "data");
        
        if (cached != null) {
            log.info("Loaded package cache from Redis for dataset: {}, page: {}", datasetName, pageIndex);
            return (ConcurrentHashMap<ZonedDateTime, Map<String, Set<String>>>) cached;
        }
        
        return null;
    }

    @SuppressWarnings("unchecked")
    public ConcurrentHashMap<ZonedDateTime, Map<String, Set<Pair<CvssSeverity, String>>>> loadFindingCache(
        String datasetName,
        Integer pageIndex
    ) {
        String key = datasetName + ":" + pageIndex;
        Object cached = redisTemplate.opsForHash().get(FINDING_CACHE_KEY + key, "data");
        
        if (cached != null) {
            log.info("Loaded finding cache from Redis for dataset: {}, page: {}", datasetName, pageIndex);
            return (ConcurrentHashMap<ZonedDateTime, Map<String, Set<Pair<CvssSeverity, String>>>>) cached;
        }
        
        return null;
    }

    @SuppressWarnings("unchecked")
    public Map<ZonedDateTime, Set<Pair<String, String>>> loadEditCache(
        String datasetName,
        Integer pageIndex
    ) {
        String key = datasetName + ":" + pageIndex;
        Object cached = redisTemplate.opsForHash().get(EDIT_CACHE_KEY + key, "data");
        
        if (cached != null) {
            log.info("Loaded edit cache from Redis for dataset: {}, page: {}", datasetName, pageIndex);
            return (Map<ZonedDateTime, Set<Pair<String, String>>>) cached;
        }
        
        return null;
    }

    @SuppressWarnings("unchecked")
    public Set<String> loadPurlsWithFindingsCache(
        String datasetName,
        Integer pageIndex
    ) {
        String key = datasetName + ":" + pageIndex;
        Object cached = redisTemplate.opsForHash().get(PURLS_WITH_FINDINGS_CACHE_KEY + key, "data");
        
        if (cached != null) {
            log.info("Loaded purls-with-findings cache from Redis for dataset: {}, page: {}", datasetName, pageIndex);
            return (Set<String>) cached;
        }
        
        return null;
    }

    @SuppressWarnings("unchecked")
    public Map<String, ZonedDateTime> loadBacklogFirstAppearanceCache(
        String datasetName,
        Integer pageIndex
    ) {
        String key = datasetName + ":" + pageIndex;
        Object cached = redisTemplate.opsForHash().get(BACKLOG_FIRST_APPEARANCE_CACHE_KEY + key, "data");
        
        if (cached != null) {
            log.info("Loaded backlog first appearance cache from Redis for dataset: {}, page: {}", datasetName, pageIndex);
            return (Map<String, ZonedDateTime>) cached;
        }
        
        return null;
    }
}
