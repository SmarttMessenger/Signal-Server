package com.smarttmessenger.communicationwindow.model;

import java.time.Instant;

/** Returned by CommunicationWindowService when a message must be held. */
public record WindowHoldResult(String windowId, Instant windowOpensAt) {}
