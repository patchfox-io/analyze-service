package io.patchfox.analyze_service.services;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import io.patchfox.package_utils.util.CvssSeverity;
import io.patchfox.package_utils.util.Pair;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class CacheService {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    private static final String PACKAGE_CACHE_KEY = "tabulate:packages:";
    private static final String FINDING_CACHE_KEY = "tabulate:findings:";
    private static final String EDIT_CACHE_KEY = "tabulate:edits:";
    private static final String PURLS_WITH_FINDINGS_CACHE_KEY = "tabulate:purlsWithFindings:";
    private static final String BACKLOG_FIRST_APPEARANCE_CACHE_KEY = "tabulate:backlogFirstAppearance:";

    private String compress(Object obj) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOut = new GZIPOutputStream(baos);
             ObjectOutputStream objectOut = new ObjectOutputStream(gzipOut)) {
            objectOut.writeObject(obj);
        }
        return java.util.Base64.getEncoder().encodeToString(baos.toByteArray());
    }

    private Object decompress(String compressed) throws Exception {
        byte[] bytes = java.util.Base64.getDecoder().decode(compressed);
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        try (GZIPInputStream gzipIn = new GZIPInputStream(bais);
             ObjectInputStream objectIn = new ObjectInputStream(gzipIn)) {
            return objectIn.readObject();
        }
    }

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
        
        log.info("Saving caches to Redis for dataset: {}, page: {} (with gzip compression)", datasetName, pageIndex);
        
        // Delete previous page's cache to avoid accumulation
        if (pageIndex > 0) {
            String prevKey = datasetName + ":" + (pageIndex - 1);
            stringRedisTemplate.delete(PACKAGE_CACHE_KEY + prevKey);
            stringRedisTemplate.delete(FINDING_CACHE_KEY + prevKey);
            stringRedisTemplate.delete(EDIT_CACHE_KEY + prevKey);
            stringRedisTemplate.delete(PURLS_WITH_FINDINGS_CACHE_KEY + prevKey);
            stringRedisTemplate.delete(BACKLOG_FIRST_APPEARANCE_CACHE_KEY + prevKey);
            log.info("Deleted previous cache for page: {}", pageIndex - 1);
        }
        
        try {
            stringRedisTemplate.opsForValue().set(PACKAGE_CACHE_KEY + key, compress(packageCache));
            stringRedisTemplate.opsForValue().set(FINDING_CACHE_KEY + key, compress(findingCache));
            stringRedisTemplate.opsForValue().set(EDIT_CACHE_KEY + key, compress(editCache));
            stringRedisTemplate.opsForValue().set(PURLS_WITH_FINDINGS_CACHE_KEY + key, compress(purlsWithFindingsCache));
            stringRedisTemplate.opsForValue().set(BACKLOG_FIRST_APPEARANCE_CACHE_KEY + key, compress(backlogFirstAppearanceCache));
            
            log.info("Caches saved to Redis (compressed)");
        } catch (Exception e) {
            log.error("Failed to compress and save caches", e);
            throw new RuntimeException("Cache compression failed", e);
        }
    }

    @SuppressWarnings("unchecked")
    public ConcurrentHashMap<ZonedDateTime, Map<String, Set<String>>> loadPackageCache(
        String datasetName,
        Integer pageIndex
    ) {
        String key = datasetName + ":" + pageIndex;
        String cached = stringRedisTemplate.opsForValue().get(PACKAGE_CACHE_KEY + key);
        
        if (cached != null) {
            try {
                log.info("Loaded package cache from Redis for dataset: {}, page: {} (decompressing)", datasetName, pageIndex);
                return (ConcurrentHashMap<ZonedDateTime, Map<String, Set<String>>>) decompress(cached);
            } catch (Exception e) {
                log.error("Failed to decompress package cache, treating as cache miss", e);
                return null;
            }
        }
        
        return null;
    }

    @SuppressWarnings("unchecked")
    public ConcurrentHashMap<ZonedDateTime, Map<String, Set<Pair<CvssSeverity, String>>>> loadFindingCache(
        String datasetName,
        Integer pageIndex
    ) {
        String key = datasetName + ":" + pageIndex;
        String cached = stringRedisTemplate.opsForValue().get(FINDING_CACHE_KEY + key);
        
        if (cached != null) {
            try {
                log.info("Loaded finding cache from Redis for dataset: {}, page: {} (decompressing)", datasetName, pageIndex);
                return (ConcurrentHashMap<ZonedDateTime, Map<String, Set<Pair<CvssSeverity, String>>>>) decompress(cached);
            } catch (Exception e) {
                log.error("Failed to decompress finding cache, treating as cache miss", e);
                return null;
            }
        }
        
        return null;
    }

    @SuppressWarnings("unchecked")
    public Map<ZonedDateTime, Set<Pair<String, String>>> loadEditCache(
        String datasetName,
        Integer pageIndex
    ) {
        String key = datasetName + ":" + pageIndex;
        String cached = stringRedisTemplate.opsForValue().get(EDIT_CACHE_KEY + key);
        
        if (cached != null) {
            try {
                log.info("Loaded edit cache from Redis for dataset: {}, page: {} (decompressing)", datasetName, pageIndex);
                return (Map<ZonedDateTime, Set<Pair<String, String>>>) decompress(cached);
            } catch (Exception e) {
                log.error("Failed to decompress edit cache, treating as cache miss", e);
                return null;
            }
        }
        
        return null;
    }

    @SuppressWarnings("unchecked")
    public Set<String> loadPurlsWithFindingsCache(
        String datasetName,
        Integer pageIndex
    ) {
        String key = datasetName + ":" + pageIndex;
        String cached = stringRedisTemplate.opsForValue().get(PURLS_WITH_FINDINGS_CACHE_KEY + key);
        
        if (cached != null) {
            try {
                log.info("Loaded purls-with-findings cache from Redis for dataset: {}, page: {} (decompressing)", datasetName, pageIndex);
                return (Set<String>) decompress(cached);
            } catch (Exception e) {
                log.error("Failed to decompress purls-with-findings cache, treating as cache miss", e);
                return null;
            }
        }
        
        return null;
    }

    @SuppressWarnings("unchecked")
    public Map<String, ZonedDateTime> loadBacklogFirstAppearanceCache(
        String datasetName,
        Integer pageIndex
    ) {
        String key = datasetName + ":" + pageIndex;
        String cached = stringRedisTemplate.opsForValue().get(BACKLOG_FIRST_APPEARANCE_CACHE_KEY + key);
        
        if (cached != null) {
            try {
                log.info("Loaded backlog first appearance cache from Redis for dataset: {}, page: {} (decompressing)", datasetName, pageIndex);
                return (Map<String, ZonedDateTime>) decompress(cached);
            } catch (Exception e) {
                log.error("Failed to decompress backlog first appearance cache, treating as cache miss", e);
                return null;
            }
        }
        
        return null;
    }
}
