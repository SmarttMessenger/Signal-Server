package com.smarttmessenger.communicationwindow.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.whispersystems.textsecuregcm.entities.SendMessageResponse;

/** Extended send response that tells the sender their message is held by a communication window. */
public class SmarttSendMessageResponse extends SendMessageResponse {

  @JsonProperty
  private final boolean communicationWindowHeld;

  @JsonProperty
  private final long windowOpensAt;

  public SmarttSendMessageResponse(boolean needsSync, boolean communicationWindowHeld, long windowOpensAt) {
    super(needsSync);
    this.communicationWindowHeld = communicationWindowHeld;
    this.windowOpensAt = windowOpensAt;
  }

  public boolean isCommunicationWindowHeld() { return communicationWindowHeld; }
  public long getWindowOpensAt() { return windowOpensAt; }
}
