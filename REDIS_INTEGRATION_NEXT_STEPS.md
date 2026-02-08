# Redis Cache Integration - Remaining Steps

## ✅ Completed
1. Created `CacheService.java` - Redis operations service
2. Created `RedisConfig.java` - Spring Redis configuration  
3. Added Redis to `docker-compose.yml`
4. Added Redis dependency to `pom.xml`
5. Added `CacheService` autowiring to `TabulateService.java`

---

## 🔧 Next: Integrate Cache Hooks into TabulateService.tabulate()

### Step 1: Add Cache Loading Hook (Line ~290)

**Location:** Right after `Optional<DatasetMetrics> currentHistoricalDatasetMetricsRecordOptional = Optional.empty();`

**Add this code:**

```java
// ===== REDIS CACHE INTEGRATION: TRY LOAD FROM REDIS FIRST =====
var cachedPackages = cacheService.loadPackageCache(datasetName, pageIndex);
var cachedFindings = cacheService.loadFindingCache(datasetName, pageIndex);
var cachedEdits = cacheService.loadEditCache(datasetName, pageIndex);

boolean cacheHit = (cachedPackages != null && cachedFindings != null && cachedEdits != null);

if (cacheHit) {
    log.info("*** REDIS CACHE HIT: Loaded all caches from Redis for dataset: {}, page: {} ***", datasetName, pageIndex);
    historicalPackagePurlsByDatasourcePurl = cachedPackages;
    historicalFindingsByDatasourcePurl = cachedFindings;
    historicalDatasetEditsByCommitDateAsc = new ConcurrentHashMap<>(cachedEdits);
    
    // Still need to populate currentPackagePurlsWithFindings for continuity
    var allPackagePurls = cachedPackages.values().stream()
        .flatMap(m -> m.values().stream())
        .flatMap(Set::stream)
        .collect(Collectors.toSet());
    
    String packagePurlsString = setToSqlArrayString(allPackagePurls);
    currentPackagePurlsWithFindings = new HashSet<>(
        packageRepository.filterPackageIdsWithFindings(packagePurlsString)
    );
    log.info("Populated currentPackagePurlsWithFindings with {} packages", currentPackagePurlsWithFindings.size());
}

if (!cacheHit && !historicalDatasetMetricsRecordsByCommitDateAsc.isEmpty()) {
    log.info("*** REDIS CACHE MISS: Hydrating caches from database for dataset: {}, page: {} ***", datasetName, pageIndex);
    // EXISTING CACHE HYDRATION CODE CONTINUES HERE...
```

**Then wrap the existing cache hydration code (lines ~290-690) in:**
```java
if (!cacheHit && !historicalDatasetMetricsRecordsByCommitDateAsc.isEmpty()) {
    // ... all existing cache hydration code ...
}
```

---

### Step 2: Add Cache Saving Hook (Line ~1207)

**Location:** Right before `return ApiResponse.builder()`

**Add this code:**

```java
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
```

---

## 🧪 Testing

1. **Build:**
   ```bash
   cd /home/snerd/patchfox_git/analyze-service
   mvn clean install jib:dockerBuild
   ```

2. **Start services:**
   ```bash
   cd /home/snerd/patchfox_git/docker-compose
   docker compose up -d
   ```

3. **Verify Redis:**
   ```bash
   docker exec -it docker-compose-redis-1 redis-cli
   > PING
   PONG
   > KEYS tabulate:*
   (empty array initially)
   ```

4. **Run a tabulation job** (via orchestrate-service)

5. **Check Redis after first page:**
   ```bash
   > KEYS tabulate:*
   1) "tabulate:packages:nasa:0"
   2) "tabulate:findings:nasa:0"
   3) "tabulate:edits:nasa:0"
   ```

6. **Check logs for cache hit on page 1:**
   ```bash
   docker logs docker-compose-analyze-service-1 | grep "REDIS CACHE"
   ```

---

## 📊 Expected Performance

| Scenario | Time | Notes |
|----------|------|-------|
| Page 0 (cache miss) | 2-5 min | Normal DB hydration |
| Page 1 (cache hit) | ~50ms | Redis fetch only |
| Page 2+ (cache hit) | ~50ms | Redis fetch only |

**Total savings:** ~99% reduction in cache hydration time for pages 1+

---

## 🐛 Troubleshooting

**Cache not saving:**
- Check Redis connection: `docker logs docker-compose-redis-1`
- Check analyze-service logs for serialization errors

**Cache not loading:**
- Verify keys exist: `redis-cli KEYS tabulate:*`
- Check for deserialization errors in logs

**Memory issues:**
- Redis configured with 2GB max + LRU eviction
- Old caches automatically evicted when full
- Monitor: `redis-cli INFO memory`

---

## 🎯 Next Steps After Integration

1. Apply the two code changes above
2. Rebuild and test
3. Monitor first multi-page job
4. Tune Redis memory if needed
5. Consider adding cache TTL (optional)
