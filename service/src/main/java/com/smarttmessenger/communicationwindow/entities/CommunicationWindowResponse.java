package com.smarttmessenger.communicationwindow.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.smarttmessenger.communicationwindow.model.CommunicationWindow;
import com.smarttmessenger.communicationwindow.model.CommunicationWindowSchedule;
import com.smarttmessenger.communicationwindow.model.WindowExpectations;
import java.util.List;
import java.util.Set;

public class CommunicationWindowResponse {

  @JsonProperty private String windowId;
  @JsonProperty private String name;
  @JsonProperty private String emoji;
  @JsonProperty private boolean enabled;
  @JsonProperty private List<CommunicationWindowSchedule> schedules;
  @JsonProperty private Set<String> exceptionContacts;
  @JsonProperty private boolean allowCallsFromExceptions;
  @JsonProperty private boolean allowCallsFromAll;
  @JsonProperty private WindowExpectations expectations;

  public static CommunicationWindowResponse from(CommunicationWindow window) {
    CommunicationWindowResponse r = new CommunicationWindowResponse();
    r.windowId = window.getWindowId();
    r.name = window.getName();
    r.emoji = window.getEmoji();
    r.enabled = window.isEnabled();
    r.schedules = window.getSchedules();
    r.exceptionContacts = window.getExceptionContacts();
    r.allowCallsFromExceptions = window.isAllowCallsFromExceptions();
    r.allowCallsFromAll = window.isAllowCallsFromAll();
    r.expectations = window.getExpectations();
    return r;
  }

  public String getWindowId() { return windowId; }
}
