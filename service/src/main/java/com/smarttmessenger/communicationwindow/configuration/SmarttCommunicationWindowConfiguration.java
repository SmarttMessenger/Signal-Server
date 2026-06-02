package com.smarttmessenger.communicationwindow.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

public class SmarttCommunicationWindowConfiguration {

  @NotEmpty
  @JsonProperty
  private String windowsTableName = "smartt_communication_windows";

  @NotEmpty
  @JsonProperty
  private String heldMessagesTableName = "smartt_held_messages";

  public String getWindowsTableName() { return windowsTableName; }
  public String getHeldMessagesTableName() { return heldMessagesTableName; }
}
