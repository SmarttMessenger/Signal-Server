package com.smarttmessenger.communicationwindow.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class CommunicationWindow {

  @JsonProperty
  private String windowId;

  @JsonProperty
  private String name;

  @JsonProperty
  private String emoji;

  @JsonProperty
  private boolean enabled;

  @JsonProperty
  private List<CommunicationWindowSchedule> schedules;

  @JsonProperty
  private Set<String> exceptionContacts;

  @JsonProperty
  private boolean allowCallsFromExceptions;

  @JsonProperty
  private boolean allowCallsFromAll;

  @JsonProperty
  private WindowExpectations expectations;

  public CommunicationWindow() {}

  public CommunicationWindow(String windowId, String name, String emoji, boolean enabled,
      List<CommunicationWindowSchedule> schedules, Set<String> exceptionContacts,
      boolean allowCallsFromExceptions, boolean allowCallsFromAll, WindowExpectations expectations) {
    this.windowId = windowId;
    this.name = name;
    this.emoji = emoji;
    this.enabled = enabled;
    this.schedules = schedules;
    this.exceptionContacts = exceptionContacts;
    this.allowCallsFromExceptions = allowCallsFromExceptions;
    this.allowCallsFromAll = allowCallsFromAll;
    this.expectations = expectations;
  }

  /** Returns the active schedule at the given time, if any. */
  public Optional<CommunicationWindowSchedule> activeSchedule(ZonedDateTime now) {
    if (!enabled) return Optional.empty();
    return schedules.stream().filter(s -> s.isCurrentlyActive(now)).findFirst();
  }

  /** Returns when the currently active schedule ends, i.e. when messages will be delivered. */
  public Optional<Instant> windowOpensAt(ZoneId timezone, java.time.Clock clock) {
    ZonedDateTime now = ZonedDateTime.now(clock.withZone(timezone));
    return activeSchedule(now).map(schedule -> {
      int endMinutes = schedule.getEnd();
      LocalTime endTime = LocalTime.of(endMinutes / 60, endMinutes % 60);
      ZonedDateTime endDateTime = now.with(endTime);

      if (schedule.getEnd() <= schedule.getStart()) {
        // Wraparound (e.g. 22:00–06:00): end is next day if already past midnight
        if (!endDateTime.isAfter(now)) {
          endDateTime = endDateTime.plusDays(1);
        }
      }
      return endDateTime.toInstant();
    });
  }

  public boolean isSenderException(String senderAci) {
    return exceptionContacts != null && exceptionContacts.contains(senderAci);
  }

  public String getWindowId() { return windowId; }
  public String getName() { return name; }
  public String getEmoji() { return emoji; }
  public boolean isEnabled() { return enabled; }
  public List<CommunicationWindowSchedule> getSchedules() { return schedules; }
  public Set<String> getExceptionContacts() { return exceptionContacts; }
  public boolean isAllowCallsFromExceptions() { return allowCallsFromExceptions; }
  public boolean isAllowCallsFromAll() { return allowCallsFromAll; }
  public WindowExpectations getExpectations() { return expectations; }

  public void setWindowId(String windowId) { this.windowId = windowId; }
  public void setEnabled(boolean enabled) { this.enabled = enabled; }
}
