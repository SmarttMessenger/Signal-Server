package com.smarttmessenger.communicationwindow.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ByteArraySerializer;
import com.webauthn4j.converter.jackson.deserializer.json.ByteArrayBase64Deserializer;

/** Serializable representation of one IncomingMessage, stored while held. */
public class HeldIncomingMessage {

  @JsonProperty
  private int type;

  @JsonProperty
  private byte destinationDeviceId;

  @JsonProperty
  private int destinationRegistrationId;

  @JsonProperty
  @JsonSerialize(using = ByteArraySerializer.class)
  @JsonDeserialize(using = ByteArrayBase64Deserializer.class)
  private byte[] content;

  public HeldIncomingMessage() {}

  public HeldIncomingMessage(int type, byte destinationDeviceId, int destinationRegistrationId, byte[] content) {
    this.type = type;
    this.destinationDeviceId = destinationDeviceId;
    this.destinationRegistrationId = destinationRegistrationId;
    this.content = content;
  }

  public int getType() { return type; }
  public byte getDestinationDeviceId() { return destinationDeviceId; }
  public int getDestinationRegistrationId() { return destinationRegistrationId; }
  public byte[] getContent() { return content; }
}
