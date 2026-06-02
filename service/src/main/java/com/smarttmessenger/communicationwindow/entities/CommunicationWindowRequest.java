package com.smarttmessenger.communicationwindow.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.smarttmessenger.communicationwindow.model.CommunicationWindowSchedule;
import com.smarttmessenger.communicationwindow.model.WindowExpectations;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.Set;

public class CommunicationWindowRequest {

  @NotBlank
  @JsonProperty
  private String name;

  @JsonProperty
  private String emoji;

  @JsonProperty
  private boolean enabled = true;

  @NotNull
  @Valid
  @JsonProperty
  private List<CommunicationWindowSchedule> schedules;

  @JsonProperty
  private Set<String> exceptionContacts;

  @JsonProperty
  private boolean allowCallsFromExceptions;

  @JsonProperty
  private boolean allowCallsFromAll;

  @Valid
  @JsonProperty
  private WindowExpectations expectations;

  public String getName() { return name; }
  public String getEmoji() { return emoji; }
  public boolean isEnabled() { return enabled; }
  public List<CommunicationWindowSchedule> getSchedules() { return schedules; }
  public Set<String> getExceptionContacts() { return exceptionContacts; }
  public boolean isAllowCallsFromExceptions() { return allowCallsFromExceptions; }
  public boolean isAllowCallsFromAll() { return allowCallsFromAll; }
  public WindowExpectations getExpectations() { return expectations; }
}
