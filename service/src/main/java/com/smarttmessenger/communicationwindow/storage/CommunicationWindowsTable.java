package com.smarttmessenger.communicationwindow.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.smarttmessenger.communicationwindow.model.CommunicationWindow;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.Util;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import java.io.IOException;
import java.util.*;

/**
 * DynamoDB table for communication window definitions.
 *
 * Schema:
 *   PK (A): account UUID string
 *   SK (W): window ID string (UUID)
 *   D:      JSON-serialized CommunicationWindow
 */
public class CommunicationWindowsTable {

  static final String ATTR_ACCOUNT_UUID = "A";
  static final String ATTR_WINDOW_ID = "W";
  static final String ATTR_DATA = "D";

  private final DynamoDbClient dynamoDbClient;
  private final String tableName;

  public CommunicationWindowsTable(DynamoDbClient dynamoDbClient, String tableName) {
    this.dynamoDbClient = dynamoDbClient;
    this.tableName = tableName;
  }

  public void put(String accountUuid, CommunicationWindow window) {
    try {
      dynamoDbClient.putItem(PutItemRequest.builder()
          .tableName(tableName)
          .item(Map.of(
              ATTR_ACCOUNT_UUID, AttributeValue.fromS(accountUuid),
              ATTR_WINDOW_ID, AttributeValue.fromS(window.getWindowId()),
              ATTR_DATA, AttributeValue.fromS(SystemMapper.jsonMapper().writeValueAsString(window))
          ))
          .build());
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize CommunicationWindow", e);
    }
  }

  public List<CommunicationWindow> getAll(String accountUuid) {
    QueryResponse response = dynamoDbClient.query(QueryRequest.builder()
        .tableName(tableName)
        .keyConditionExpression("#a = :a")
        .expressionAttributeNames(Map.of("#a", ATTR_ACCOUNT_UUID))
        .expressionAttributeValues(Map.of(":a", AttributeValue.fromS(accountUuid)))
        .build());

    List<CommunicationWindow> windows = new ArrayList<>();
    for (Map<String, AttributeValue> item : response.items()) {
      try {
        windows.add(SystemMapper.jsonMapper().readValue(
            item.get(ATTR_DATA).s(), CommunicationWindow.class));
      } catch (IOException e) {
        throw new RuntimeException("Failed to deserialize CommunicationWindow", e);
      }
    }
    return windows;
  }

  public Optional<CommunicationWindow> get(String accountUuid, String windowId) {
    GetItemResponse response = dynamoDbClient.getItem(GetItemRequest.builder()
        .tableName(tableName)
        .key(Map.of(
            ATTR_ACCOUNT_UUID, AttributeValue.fromS(accountUuid),
            ATTR_WINDOW_ID, AttributeValue.fromS(windowId)
        ))
        .build());

    if (!response.hasItem()) return Optional.empty();
    try {
      return Optional.of(SystemMapper.jsonMapper().readValue(
          response.item().get(ATTR_DATA).s(), CommunicationWindow.class));
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize CommunicationWindow", e);
    }
  }

  public void delete(String accountUuid, String windowId) {
    dynamoDbClient.deleteItem(DeleteItemRequest.builder()
        .tableName(tableName)
        .key(Map.of(
            ATTR_ACCOUNT_UUID, AttributeValue.fromS(accountUuid),
            ATTR_WINDOW_ID, AttributeValue.fromS(windowId)
        ))
        .build());
  }
}
