package com.smarttmessenger.communicationwindow.cache;

import io.lettuce.core.RedisException;
import io.lettuce.core.SetArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;

import java.time.Duration;
import java.util.Optional;

/**
 * Caches whether an account has any communication windows at all.
 *
 * <p>The window check runs on the send hot path for every identified-sender individual message, and
 * the underlying {@code windowsTable.getAll} is a blocking DynamoDB query. The overwhelming majority
 * of accounts have never created a window, so caching that single fact turns the common case from a
 * DynamoDB query into one Redis GET. Signal's own send path touches no DynamoDB at all, so this keeps
 * our addition to the hot path proportionate.
 *
 * <p>Redis failures are treated as cache misses: the caller falls back to the query, so an outage
 * degrades to the previous behaviour rather than silently skipping the window check.
 */
public class WindowPresenceCache {

  private static final Logger logger = LoggerFactory.getLogger(WindowPresenceCache.class);

  private static final String CACHE_PREFIX = "smartt_cw_any::";

  /** Bounds staleness if an invalidation is ever lost (e.g. Redis restarted mid-write). */
  private static final Duration TTL = Duration.ofHours(6);

  private static final String PRESENT = "1";
  private static final String ABSENT = "0";

  private final FaultTolerantRedisClusterClient cacheCluster;

  public WindowPresenceCache(final FaultTolerantRedisClusterClient cacheCluster) {
    this.cacheCluster = cacheCluster;
  }

  /**
   * Returns {@code true}/{@code false} if we know whether the account has windows, or empty if the
   * cache cannot answer and the caller must query.
   */
  public Optional<Boolean> hasWindows(final String accountUuid) {
    try {
      final String value = cacheCluster.withCluster(connection -> connection.sync().get(cacheKey(accountUuid)));

      if (PRESENT.equals(value)) {
        return Optional.of(true);
      }
      if (ABSENT.equals(value)) {
        return Optional.of(false);
      }
      return Optional.empty();
    } catch (final RedisException e) {
      logger.warn("Failed to read window presence from Redis for account={}", accountUuid, e);
      return Optional.empty();
    }
  }

  /**
   * Records whether the account has windows. Called both to populate after a query and to invalidate
   * after a window is created, updated or deleted — writing the new value rather than deleting the
   * key so a window change can't send every subsequent message back to DynamoDB.
   */
  public void set(final String accountUuid, final boolean hasWindows) {
    try {
      cacheCluster.useCluster(connection -> connection.sync()
          .set(cacheKey(accountUuid), hasWindows ? PRESENT : ABSENT, SetArgs.Builder.ex(TTL)));
    } catch (final RedisException e) {
      // Non-fatal: the next send simply queries DynamoDB again.
      logger.warn("Failed to cache window presence for account={}", accountUuid, e);
    }
  }

  private static String cacheKey(final String accountUuid) {
    return CACHE_PREFIX + accountUuid;
  }
}
