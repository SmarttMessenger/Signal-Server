package com.smarttmessenger.communicationwindow.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.smarttmessenger.communicationwindow.model.HeldMessageData;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * DynamoDB table for messages held while a recipient's communication window is closed.
 *
 * Schema:
 *   PK (R): recipient account UUID string
 *   SK (M): "{deliverAtMs}#{messageUuid}" — unique per held message; one delivery job per key
 *   D:      JSON-serialized HeldMessageData
 *   E:      TTL (epoch seconds) — reuses Signal's offline-message retention (dynamoDbTables.messages.expiration)
 */
public class HeldMessagesTable {

  private static final String ATTR_RECIPIENT_UUID = "R";
  private static final String ATTR_SORT_KEY = "M";
  private static final String ATTR_DATA = "D";
  private static final String ATTR_TTL = "E";

  private final DynamoDbClient dynamoDbClient;
  private final String tableName;
  private final Duration timeToLive;

  public HeldMessagesTable(DynamoDbClient dynamoDbClient, String tableName, Duration timeToLive) {
    this.dynamoDbClient = dynamoDbClient;
    this.tableName = tableName;
    this.timeToLive = timeToLive;
  }

  /** Stores a held message and returns its generated sort key, which keys the delivery job. */
  public String store(String recipientUuid, Instant deliverAt, HeldMessageData messageData) {
    String sortKey = buildSortKey(deliverAt, UUID.randomUUID().toString());
    long ttl = Instant.now().getEpochSecond() + timeToLive.getSeconds();

    try {
      dynamoDbClient.putItem(PutItemRequest.builder()
          .tableName(tableName)
          .item(Map.of(
              ATTR_RECIPIENT_UUID, AttributeValue.fromS(recipientUuid),
              ATTR_SORT_KEY, AttributeValue.fromS(sortKey),
              ATTR_DATA, AttributeValue.fromS(SystemMapper.jsonMapper().writeValueAsString(messageData)),
              ATTR_TTL, AttributeValue.fromN(String.valueOf(ttl))
          ))
          .build());
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize HeldMessageData", e);
    }
    return sortKey;
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

  /** Returns the sort keys of all messages currently held for the recipient. */
  public java.util.List<String> getSortKeys(String recipientUuid) {
    final java.util.List<String> sortKeys = new java.util.ArrayList<>();
    Map<String, AttributeValue> lastEvaluatedKey = null;

    do {
      QueryRequest.Builder builder = QueryRequest.builder()
          .tableName(tableName)
          .keyConditionExpression("#r = :r")
          .expressionAttributeNames(Map.of("#r", ATTR_RECIPIENT_UUID, "#m", ATTR_SORT_KEY))
          .expressionAttributeValues(Map.of(":r", AttributeValue.fromS(recipientUuid)))
          .projectionExpression("#m");
      if (lastEvaluatedKey != null) {
        builder.exclusiveStartKey(lastEvaluatedKey);
      }

      QueryResponse response = dynamoDbClient.query(builder.build());
      response.items().forEach(item -> sortKeys.add(item.get(ATTR_SORT_KEY).s()));
      lastEvaluatedKey = response.hasLastEvaluatedKey() && !response.lastEvaluatedKey().isEmpty()
          ? response.lastEvaluatedKey() : null;
    } while (lastEvaluatedKey != null);

    return sortKeys;
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

  private static String buildSortKey(Instant deliverAt, String messageUuid) {
    return String.format("%019d#%s", deliverAt.toEpochMilli(), messageUuid);
  }

  public record HeldMessageEntry(String recipientUuid, String sortKey, HeldMessageData data) {}
}
