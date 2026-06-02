package com.smarttmessenger.communicationwindow.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;

/** Sender-visible expectations set by the window owner. */
public class WindowExpectations {

  public enum CheckFrequency {
    EVERY_FEW_MINUTES, ONCE_AN_HOUR, EVERY_FEW_HOURS, ONCE_A_DAY
  }

  public enum UsualReplyTime {
    WITHIN_MINUTES, WITHIN_AN_HOUR, IN_THE_AFTERNOON, IN_THE_EVENING, NEXT_DAY, NEXT_BUSINESS_DAY
  }

  @JsonProperty
  @Nullable
  private CheckFrequency checkFrequency;

  @JsonProperty
  @Nullable
  private UsualReplyTime usualReplyTime;

  @JsonProperty
  @Nullable
  private String personalNote;

  public WindowExpectations() {}

  public WindowExpectations(CheckFrequency checkFrequency, UsualReplyTime usualReplyTime, String personalNote) {
    this.checkFrequency = checkFrequency;
    this.usualReplyTime = usualReplyTime;
    this.personalNote = personalNote;
  }

  @Nullable public CheckFrequency getCheckFrequency() { return checkFrequency; }
  @Nullable public UsualReplyTime getUsualReplyTime() { return usualReplyTime; }
  @Nullable public String getPersonalNote() { return personalNote; }
}
