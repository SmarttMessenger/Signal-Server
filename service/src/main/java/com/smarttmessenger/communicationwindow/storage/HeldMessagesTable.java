package com.smarttmessenger.communicationwindow.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.smarttmessenger.communicationwindow.model.HeldMessageData;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * DynamoDB table for messages held while a recipient's communication window is closed.
 *
 * Schema:
 *   PK (R): recipient account UUID string
 *   SK (M): "{deliverAtMs}#{heldAtMs}#{messageUuid}" for a held message, "JOB#{deliverAtMs}" for a
 *           per-window-opening drain marker, or "LOCK" for the recipient's drain lease
 *   D:      JSON-serialized HeldMessageData (held messages only)
 *   L:      drain lease expiry in epoch millis (LOCK only)
 *   E:      TTL (epoch seconds) — reuses Signal's offline-message retention (dynamoDbTables.messages.expiration)
 *
 * <p>Both components of a held message's sort key are zero-padded so lexicographic order is
 * numeric order: entries sort by delivery time, then by the order they arrived. That ordering is
 * what lets a drain replay a backlog in the order it was sent.
 */
public class HeldMessagesTable {

  private static final String ATTR_RECIPIENT_UUID = "R";
  private static final String ATTR_SORT_KEY = "M";
  private static final String ATTR_DATA = "D";
  private static final String ATTR_LEASE = "L";
  private static final String ATTR_TTL = "E";

  private static final String JOB_MARKER_PREFIX = "JOB#";
  private static final String LOCK_SORT_KEY = "LOCK";

  /**
   * Greater than every held-message sort key, which begins with a digit; 'J' sorts above digits, so
   * a "due entries" query bounded by a numeric prefix never returns a marker.
   */
  private static final String MAX_SUFFIX = "￿";

  private final DynamoDbClient dynamoDbClient;
  private final String tableName;
  private final Duration timeToLive;

  public HeldMessagesTable(DynamoDbClient dynamoDbClient, String tableName, Duration timeToLive) {
    this.dynamoDbClient = dynamoDbClient;
    this.tableName = tableName;
    this.timeToLive = timeToLive;
  }

  /**
   * Stores a held message and returns its generated sort key. {@code heldAt} is the server clock at
   * the moment the message arrived; it is what orders a backlog on release, so it must come from a
   * single clock rather than from any sender.
   */
  public String store(String recipientUuid, Instant deliverAt, Instant heldAt, HeldMessageData messageData) {
    String sortKey = buildSortKey(deliverAt, heldAt, UUID.randomUUID().toString());

    try {
      dynamoDbClient.putItem(PutItemRequest.builder()
          .tableName(tableName)
          .item(Map.of(
              ATTR_RECIPIENT_UUID, AttributeValue.fromS(recipientUuid),
              ATTR_SORT_KEY, AttributeValue.fromS(sortKey),
              ATTR_DATA, AttributeValue.fromS(SystemMapper.jsonMapper().writeValueAsString(messageData)),
              ATTR_TTL, AttributeValue.fromN(String.valueOf(ttlEpochSeconds()))
          ))
          .build());
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize HeldMessageData", e);
    }
    return sortKey;
  }

  /**
   * Returns every held message for the recipient that is due by {@code nowMs}, oldest arrival first.
   *
   * <p>One query replaces a per-message fetch, and the upper bound does three jobs at once: it stops
   * at messages that are not due yet, and it excludes the {@code JOB#…} markers, whose keys sort
   * above any numeric key.
   */
  public List<HeldMessageEntry> getDueEntries(String recipientUuid, long nowMs) {
    final List<HeldMessageEntry> entries = new ArrayList<>();
    final String maxDue = String.format("%019d#%s", nowMs, MAX_SUFFIX);
    Map<String, AttributeValue> lastEvaluatedKey = null;

    do {
      QueryRequest.Builder builder = QueryRequest.builder()
          .tableName(tableName)
          .keyConditionExpression("#r = :r AND #m <= :maxDue")
          .expressionAttributeNames(Map.of("#r", ATTR_RECIPIENT_UUID, "#m", ATTR_SORT_KEY))
          .expressionAttributeValues(Map.of(
              ":r", AttributeValue.fromS(recipientUuid),
              ":maxDue", AttributeValue.fromS(maxDue)));
      if (lastEvaluatedKey != null) {
        builder.exclusiveStartKey(lastEvaluatedKey);
      }

      QueryResponse response = dynamoDbClient.query(builder.build());
      for (Map<String, AttributeValue> item : response.items()) {
        final String sortKey = item.get(ATTR_SORT_KEY).s();
        final AttributeValue data = item.get(ATTR_DATA);
        if (data == null) {
          // Defensive: a marker should already be excluded by the key bound.
          continue;
        }
        try {
          entries.add(new HeldMessageEntry(recipientUuid, sortKey,
              SystemMapper.jsonMapper().readValue(data.s(), HeldMessageData.class)));
        } catch (IOException e) {
          throw new RuntimeException("Failed to deserialize HeldMessageData", e);
        }
      }
      lastEvaluatedKey = response.hasLastEvaluatedKey() && !response.lastEvaluatedKey().isEmpty()
          ? response.lastEvaluatedKey() : null;
    } while (lastEvaluatedKey != null);

    return entries;
  }

  // --- Drain coordination ---
  //
  // Two separate concerns, two separate items:
  //   JOB#{deliverAtMs}  one per window opening. Its conditional creation guarantees exactly one
  //                      delivery job is scheduled no matter how many messages are held.
  //   LOCK               one per recipient. A drain delivers every due entry regardless of which
  //                      window opening it belongs to, so the execution mutex has to be per
  //                      recipient — a per-opening lease would let two openings drain concurrently
  //                      and lose the ordering it exists to protect.

  /**
   * Creates the drain marker for a window opening, returning true if this caller created it — and is
   * therefore the one that should schedule the delivery job.
   */
  public boolean tryCreateDrainMarker(String recipientUuid, long deliverAtMs) {
    try {
      dynamoDbClient.putItem(PutItemRequest.builder()
          .tableName(tableName)
          .item(Map.of(
              ATTR_RECIPIENT_UUID, AttributeValue.fromS(recipientUuid),
              ATTR_SORT_KEY, AttributeValue.fromS(markerSortKey(deliverAtMs)),
              ATTR_TTL, AttributeValue.fromN(String.valueOf(ttlEpochSeconds()))
          ))
          .conditionExpression("attribute_not_exists(#m)")
          .expressionAttributeNames(Map.of("#m", ATTR_SORT_KEY))
          .build());
      return true;
    } catch (ConditionalCheckFailedException e) {
      return false;
    }
  }

  /**
   * Claims the exclusive right to drain this recipient until {@code leaseExpiryMs}. Fails while
   * another drain holds an unexpired lease; a lease abandoned by a dead process is reclaimable once
   * it expires. Creates the lock item if it isn't there.
   */
  public boolean tryAcquireDrainLease(String recipientUuid, long nowMs, long leaseExpiryMs) {
    try {
      dynamoDbClient.updateItem(UpdateItemRequest.builder()
          .tableName(tableName)
          .key(Map.of(
              ATTR_RECIPIENT_UUID, AttributeValue.fromS(recipientUuid),
              ATTR_SORT_KEY, AttributeValue.fromS(LOCK_SORT_KEY)
          ))
          .updateExpression("SET #l = :expiry, #e = :ttl")
          .conditionExpression("attribute_not_exists(#m) OR #l < :now")
          .expressionAttributeNames(Map.of("#m", ATTR_SORT_KEY, "#l", ATTR_LEASE, "#e", ATTR_TTL))
          .expressionAttributeValues(Map.of(
              ":expiry", AttributeValue.fromN(String.valueOf(leaseExpiryMs)),
              ":now", AttributeValue.fromN(String.valueOf(nowMs)),
              ":ttl", AttributeValue.fromN(String.valueOf(ttlEpochSeconds()))))
          .build());
      return true;
    } catch (ConditionalCheckFailedException e) {
      return false;
    }
  }

  /**
   * Releases the drain lease. Conditional on still owning it, so a holder whose lease already expired
   * and was taken over cannot free its successor's lock.
   */
  public void releaseDrainLease(String recipientUuid, long leaseExpiryMs) {
    try {
      dynamoDbClient.deleteItem(DeleteItemRequest.builder()
          .tableName(tableName)
          .key(Map.of(
              ATTR_RECIPIENT_UUID, AttributeValue.fromS(recipientUuid),
              ATTR_SORT_KEY, AttributeValue.fromS(LOCK_SORT_KEY)
          ))
          .conditionExpression("#l = :expiry")
          .expressionAttributeNames(Map.of("#l", ATTR_LEASE))
          .expressionAttributeValues(Map.of(":expiry", AttributeValue.fromN(String.valueOf(leaseExpiryMs))))
          .build());
    } catch (ConditionalCheckFailedException e) {
      // Lease already expired and was taken over; leaving it alone is the correct outcome.
    }
  }

  /** Removes the marker once the backlog for this window opening is fully delivered. */
  public void deleteDrainMarker(String recipientUuid, long deliverAtMs) {
    delete(recipientUuid, markerSortKey(deliverAtMs));
  }

  /** Fetches a single held message by its sort key. Empty if already delivered or TTL-expired. */
  public Optional<HeldMessageEntry> getEntry(String recipientUuid, String sortKey) {
    GetItemResponse response = dynamoDbClient.getItem(GetItemRequest.builder()
        .tableName(tableName)
        .key(Map.of(
            ATTR_RECIPIENT_UUID, AttributeValue.fromS(recipientUuid),
            ATTR_SORT_KEY, AttributeValue.fromS(sortKey)
        ))
        .build());

    if (!response.hasItem()) {
      return Optional.empty();
    }

    try {
      HeldMessageData data = SystemMapper.jsonMapper().readValue(
          response.item().get(ATTR_DATA).s(), HeldMessageData.class);
      return Optional.of(new HeldMessageEntry(recipientUuid, sortKey, data));
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize HeldMessageData", e);
    }
  }

  public void delete(String recipientUuid, String sortKey) {
    dynamoDbClient.deleteItem(DeleteItemRequest.builder()
        .tableName(tableName)
        .key(Map.of(
            ATTR_RECIPIENT_UUID, AttributeValue.fromS(recipientUuid),
            ATTR_SORT_KEY, AttributeValue.fromS(sortKey)
        ))
        .build());
  }

  private static String buildSortKey(Instant deliverAt, Instant heldAt, String messageUuid) {
    return String.format("%019d#%019d#%s", deliverAt.toEpochMilli(), heldAt.toEpochMilli(), messageUuid);
  }

  private static String markerSortKey(long deliverAtMs) {
    return JOB_MARKER_PREFIX + deliverAtMs;
  }

  private long ttlEpochSeconds() {
    return Instant.now().getEpochSecond() + timeToLive.getSeconds();
  }

  public record HeldMessageEntry(String recipientUuid, String sortKey, HeldMessageData data) {}
}
