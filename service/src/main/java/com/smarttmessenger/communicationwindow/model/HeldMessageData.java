package com.smarttmessenger.communicationwindow.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** Full payload stored in DynamoDB for a held message, used to reconstruct delivery. */
public class HeldMessageData {

  @JsonProperty
  private String destinationServiceId;

  @JsonProperty
  private String senderAci;

  @JsonProperty
  private byte senderDeviceId;

  @JsonProperty
  private long clientTimestamp;

  @JsonProperty
  private boolean urgent;

  @JsonProperty
  private boolean story;

  @JsonProperty
  private boolean online;

  @JsonProperty
  private List<HeldIncomingMessage> messages;

  public HeldMessageData() {}

  public HeldMessageData(String destinationServiceId, String senderAci, byte senderDeviceId,
      long clientTimestamp, boolean urgent, boolean story, boolean online,
      List<HeldIncomingMessage> messages) {
    this.destinationServiceId = destinationServiceId;
    this.senderAci = senderAci;
    this.senderDeviceId = senderDeviceId;
    this.clientTimestamp = clientTimestamp;
    this.urgent = urgent;
    this.story = story;
    this.online = online;
    this.messages = messages;
  }

  public String getDestinationServiceId() { return destinationServiceId; }
  public String getSenderAci() { return senderAci; }
  public byte getSenderDeviceId() { return senderDeviceId; }
  public long getClientTimestamp() { return clientTimestamp; }
  public boolean isUrgent() { return urgent; }
  public boolean isStory() { return story; }
  public boolean isOnline() { return online; }
  public List<HeldIncomingMessage> getMessages() { return messages; }
}
