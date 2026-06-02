package com.smarttmessenger.communicationwindow.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.smarttmessenger.communicationwindow.model.HeldMessageData;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * DynamoDB table for messages held while a recipient's communication window is closed.
 *
 * Schema:
 *   PK (R): recipient account UUID string
 *   SK (M): "{deliverAtMs}#{messageUuid}" — allows range query for due messages
 *   D:      JSON-serialized HeldMessageData
 *   E:      TTL (epoch seconds, 7-day fallback)
 */
public class HeldMessagesTable {

  private static final String ATTR_RECIPIENT_UUID = "R";
  private static final String ATTR_SORT_KEY = "M";
  private static final String ATTR_DATA = "D";
  private static final String ATTR_TTL = "E";

  private static final long TTL_SECONDS = 7 * 24 * 60 * 60; // 7 days

  private final DynamoDbClient dynamoDbClient;
  private final String tableName;

  public HeldMessagesTable(DynamoDbClient dynamoDbClient, String tableName) {
    this.dynamoDbClient = dynamoDbClient;
    this.tableName = tableName;
  }

  public void store(String recipientUuid, Instant deliverAt, HeldMessageData messageData) {
    String sortKey = buildSortKey(deliverAt, UUID.randomUUID().toString());
    long ttl = Instant.now().getEpochSecond() + TTL_SECONDS;

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
  }

  /** Returns all held messages for a recipient whose deliverAt is at or before the given instant. */
  public List<HeldMessageEntry> getMessagesForDelivery(String recipientUuid, Instant upTo) {
    QueryResponse response = dynamoDbClient.query(QueryRequest.builder()
        .tableName(tableName)
        .keyConditionExpression("#r = :r AND #m <= :maxKey")
        .expressionAttributeNames(Map.of(
            "#r", ATTR_RECIPIENT_UUID,
            "#m", ATTR_SORT_KEY
        ))
        .expressionAttributeValues(Map.of(
            ":r", AttributeValue.fromS(recipientUuid),
            ":maxKey", AttributeValue.fromS(buildMaxSortKey(upTo))
        ))
        .build());

    List<HeldMessageEntry> entries = new ArrayList<>();
    for (Map<String, AttributeValue> item : response.items()) {
      try {
        HeldMessageData data = SystemMapper.jsonMapper().readValue(
            item.get(ATTR_DATA).s(), HeldMessageData.class);
        entries.add(new HeldMessageEntry(recipientUuid, item.get(ATTR_SORT_KEY).s(), data));
      } catch (IOException e) {
        throw new RuntimeException("Failed to deserialize HeldMessageData", e);
      }
    }
    return entries;
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

  private static String buildMaxSortKey(Instant upTo) {
    return String.format("%019d#￿", upTo.toEpochMilli());
  }

  public record HeldMessageEntry(String recipientUuid, String sortKey, HeldMessageData data) {}
}
