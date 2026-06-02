package com.smarttmessenger.communicationwindow.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.smarttmessenger.communicationwindow.model.WindowExpectations;
import javax.annotation.Nullable;

/**
 * What the sender sees when they open a conversation with a user who has an active window.
 * Shown in the conversation banner.
 */
public class WindowMetadataResponse {

  @JsonProperty private boolean windowActive;
  @JsonProperty private int windowStartMinutes;
  @JsonProperty private int windowEndMinutes;
  @JsonProperty @Nullable private WindowExpectations expectations;
  @JsonProperty @Nullable private String windowName;

  public static WindowMetadataResponse active(int startMinutes, int endMinutes,
      String windowName, WindowExpectations expectations) {
    WindowMetadataResponse r = new WindowMetadataResponse();
    r.windowActive = true;
    r.windowStartMinutes = startMinutes;
    r.windowEndMinutes = endMinutes;
    r.windowName = windowName;
    r.expectations = expectations;
    return r;
  }

  public static WindowMetadataResponse inactive() {
    WindowMetadataResponse r = new WindowMetadataResponse();
    r.windowActive = false;
    return r;
  }

  public boolean isWindowActive() { return windowActive; }
  public int getWindowStartMinutes() { return windowStartMinutes; }
  public int getWindowEndMinutes() { return windowEndMinutes; }
  @Nullable public WindowExpectations getExpectations() { return expectations; }
  @Nullable public String getWindowName() { return windowName; }
}
