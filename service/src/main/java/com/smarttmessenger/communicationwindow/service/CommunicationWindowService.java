package com.smarttmessenger.communicationwindow.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberToTimeZonesMapper;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import com.smarttmessenger.communicationwindow.model.*;
import com.smarttmessenger.communicationwindow.scheduler.HeldMessageDeliveryScheduler;
import com.smarttmessenger.communicationwindow.storage.CommunicationWindowsTable;
import com.smarttmessenger.communicationwindow.storage.HeldMessagesTable;
import org.whispersystems.textsecuregcm.entities.IncomingMessageList;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.util.*;
import java.util.stream.Collectors;

public class CommunicationWindowService {

  private static final Logger logger = LoggerFactory.getLogger(CommunicationWindowService.class);

  private final AccountsManager accountsManager;
  private final CommunicationWindowsTable windowsTable;
  private final HeldMessagesTable heldMessagesTable;
  private final HeldMessageDeliveryScheduler deliveryScheduler;
  private final Clock clock;

  public CommunicationWindowService(AccountsManager accountsManager,
      CommunicationWindowsTable windowsTable,
      HeldMessagesTable heldMessagesTable,
      HeldMessageDeliveryScheduler deliveryScheduler,
      Clock clock) {
    this.accountsManager = accountsManager;
    this.windowsTable = windowsTable;
    this.heldMessagesTable = heldMessagesTable;
    this.deliveryScheduler = deliveryScheduler;
    this.clock = clock;
  }

  /**
   * Checks whether a message to the given destination should be held, and if so, stores it and
   * schedules delivery. Returns a hold result if the message was held, empty otherwise.
   *
   * Only applies to identified-sender individual messages (V1). Sealed sender messages are not held.
   */
  public Optional<WindowHoldResult> checkAndHold(ServiceIdentifier destinationIdentifier,
      UUID senderAci,
      byte senderDeviceId,
      IncomingMessageList messages) {

    Account recipient = accountsManager.getByServiceIdentifier(destinationIdentifier).orElse(null);
    if (recipient == null) return Optional.empty();

    String recipientUuid = recipient.getIdentifier(IdentityType.ACI).toString();
    List<CommunicationWindow> windows = windowsTable.getAll(recipientUuid);
    if (windows.isEmpty()) return Optional.empty();

    ZoneId timezone = deriveTimezone(recipient);
    ZonedDateTime now = ZonedDateTime.now(clock.withZone(timezone));

    for (CommunicationWindow window : windows) {
      Optional<CommunicationWindowSchedule> activeSchedule = window.activeSchedule(now);
      if (activeSchedule.isEmpty()) continue;

      if (window.isSenderException(senderAci.toString())) {
        return Optional.empty();
      }

      Instant opensAt = window.windowOpensAt(timezone, clock)
          .orElse(clock.instant().plusSeconds(3600));

      HeldMessageData messageData = buildHeldMessageData(
          destinationIdentifier, senderAci, senderDeviceId, messages);

      heldMessagesTable.store(recipientUuid, opensAt, messageData);

      try {
        deliveryScheduler.scheduleDelivery(recipientUuid, opensAt);
      } catch (Exception e) {
        logger.warn("Failed to schedule delivery for held message (recipient={})", recipientUuid, e);
      }

      return Optional.of(new WindowHoldResult(window.getWindowId(), opensAt));
    }

    return Optional.empty();
  }

  /** Returns the active window metadata visible to senders. */
  public Optional<CommunicationWindow> getActiveWindowForSender(ServiceIdentifier destinationIdentifier) {
    Account recipient = accountsManager.getByServiceIdentifier(destinationIdentifier).orElse(null);
    if (recipient == null) return Optional.empty();

    String recipientUuid = recipient.getIdentifier(IdentityType.ACI).toString();
    List<CommunicationWindow> windows = windowsTable.getAll(recipientUuid);

    ZoneId timezone = deriveTimezone(recipient);
    ZonedDateTime now = ZonedDateTime.now(clock.withZone(timezone));

    return windows.stream()
        .filter(w -> w.activeSchedule(now).isPresent())
        .findFirst();
  }

  // --- Window CRUD ---

  public CommunicationWindow createWindow(String accountUuid, CommunicationWindow window) {
    window.setWindowId(UUID.randomUUID().toString());
    windowsTable.put(accountUuid, window);
    return window;
  }

  public Optional<CommunicationWindow> updateWindow(String accountUuid, String windowId,
      CommunicationWindow updated) {
    Optional<CommunicationWindow> existing = windowsTable.get(accountUuid, windowId);
    if (existing.isEmpty()) return Optional.empty();
    updated.setWindowId(windowId);
    windowsTable.put(accountUuid, updated);
    return Optional.of(updated);
  }

  public boolean deleteWindow(String accountUuid, String windowId) {
    if (windowsTable.get(accountUuid, windowId).isEmpty()) return false;
    windowsTable.delete(accountUuid, windowId);
    return true;
  }

  public List<CommunicationWindow> getWindows(String accountUuid) {
    return windowsTable.getAll(accountUuid);
  }

  // --- Helpers ---

  private HeldMessageData buildHeldMessageData(ServiceIdentifier destination, UUID senderAci,
      byte senderDeviceId, IncomingMessageList messages) {

    List<HeldIncomingMessage> heldMessages = messages.messages().stream()
        .map(m -> new HeldIncomingMessage(
            m.type(), m.destinationDeviceId(), m.destinationRegistrationId(), m.content()))
        .collect(Collectors.toList());

    long clientTimestamp = messages.timestamp() == 0 ? clock.millis() : messages.timestamp();

    return new HeldMessageData(
        destination.toServiceIdentifierString(),
        senderAci.toString(),
        senderDeviceId,
        clientTimestamp,
        messages.urgent(),
        false, // isStory: only identified-sender non-story messages reach this path
        messages.online(),
        heldMessages
    );
  }

  @VisibleForTesting
  ZoneId deriveTimezone(Account account) {
    try {
      Phonenumber.PhoneNumber phoneNumber = PhoneNumberUtil.getInstance().parse(account.getNumber(), null);
      List<String> zones = PhoneNumberToTimeZonesMapper.getInstance().getTimeZonesForNumber(phoneNumber);

      if (zones.equals(List.of(PhoneNumberToTimeZonesMapper.getUnknownTimeZone()))) {
        return ZoneOffset.UTC;
      }
      return ZoneId.of(zones.get(zones.size() / 2));
    } catch (Exception e) {
      return ZoneOffset.UTC;
    }
  }
}
