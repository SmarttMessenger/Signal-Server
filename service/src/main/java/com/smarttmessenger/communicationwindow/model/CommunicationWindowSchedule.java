package com.smarttmessenger.communicationwindow.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.ZonedDateTime;
import java.util.Set;

/**
 * A single time-based schedule within a communication window.
 * Start and end are stored as minutes since midnight (0–1439).
 * daysEnabled uses ISO day values: 1=Monday … 7=Sunday.
 */
public class CommunicationWindowSchedule {

  @JsonProperty
  private boolean enabled;

  @JsonProperty
  private int start;

  @JsonProperty
  private int end;

  @JsonProperty
  private Set<Integer> daysEnabled;

  public CommunicationWindowSchedule() {}

  public CommunicationWindowSchedule(boolean enabled, int start, int end, Set<Integer> daysEnabled) {
    this.enabled = enabled;
    this.start = start;
    this.end = end;
    this.daysEnabled = daysEnabled;
  }

  public boolean isCurrentlyActive(ZonedDateTime now) {
    if (!enabled) return false;
    if (!daysEnabled.contains(now.getDayOfWeek().getValue())) return false;

    int currentMinutes = now.getHour() * 60 + now.getMinute();

    if (end <= start) {
      // Wraparound schedule (e.g. 22:00–06:00): active after start OR before end
      return currentMinutes >= start || currentMinutes < end;
    }
    return currentMinutes >= start && currentMinutes < end;
  }

  public boolean isEnabled() { return enabled; }
  public int getStart() { return start; }
  public int getEnd() { return end; }
  public Set<Integer> getDaysEnabled() { return daysEnabled; }
}
