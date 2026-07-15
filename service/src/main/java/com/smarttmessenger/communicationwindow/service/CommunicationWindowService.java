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

      return Optional.of(holdAndSchedule(recipientUuid, window, timezone,
          destinationIdentifier, senderAci.toString(), senderDeviceId, messages));
    }

    return Optional.empty();
  }

  /**
   * Sealed-sender variant. The server can't read the sender in this path, so the decision is based on
   * the recipient's window alone:
   *  - window active AND exception contacts defined -> REQUIRE_IDENTIFIED (we can't honor exceptions
   *    anonymously, so the client is asked to resend as an identified sender via a 401 retry),
   *  - window active with NO exceptions -> HELD (held anonymously; sender identity not needed),
   *  - otherwise -> DELIVER (send normally, still sealed).
   */
  public SealedSenderOutcome checkAndHoldSealedSender(ServiceIdentifier destinationIdentifier,
      IncomingMessageList messages) {

    Account recipient = accountsManager.getByServiceIdentifier(destinationIdentifier).orElse(null);
    if (recipient == null) return new SealedSenderOutcome(SealedSenderAction.DELIVER, Optional.empty());

    String recipientUuid = recipient.getIdentifier(IdentityType.ACI).toString();
    List<CommunicationWindow> windows = windowsTable.getAll(recipientUuid);
    if (windows.isEmpty()) return new SealedSenderOutcome(SealedSenderAction.DELIVER, Optional.empty());

    ZoneId timezone = deriveTimezone(recipient);
    ZonedDateTime now = ZonedDateTime.now(clock.withZone(timezone));

    for (CommunicationWindow window : windows) {
      if (window.activeSchedule(now).isEmpty()) continue;

      Set<String> exceptions = window.getExceptionContacts();
      if (exceptions != null && !exceptions.isEmpty()) {
        return new SealedSenderOutcome(SealedSenderAction.REQUIRE_IDENTIFIED, Optional.empty());
      }

      WindowHoldResult result = holdAndSchedule(recipientUuid, window, timezone,
          destinationIdentifier, null, (byte) 0, messages);
      return new SealedSenderOutcome(SealedSenderAction.HELD, Optional.of(result));
    }

    return new SealedSenderOutcome(SealedSenderAction.DELIVER, Optional.empty());
  }

  public enum SealedSenderAction { DELIVER, REQUIRE_IDENTIFIED, HELD }

  public record SealedSenderOutcome(SealedSenderAction action, Optional<WindowHoldResult> holdResult) {}

  /**
   * Returns the currently-active window for a sender to display, evaluated entirely in the
   * recipient's timezone. The returned start/end belong to the schedule that is active *now*,
   * so callers must not re-evaluate the schedule in a different timezone.
   */
  public Optional<ActiveWindowInfo> getActiveWindowForSender(ServiceIdentifier destinationIdentifier) {
    Account recipient = accountsManager.getByServiceIdentifier(destinationIdentifier).orElse(null);
    if (recipient == null) return Optional.empty();

    String recipientUuid = recipient.getIdentifier(IdentityType.ACI).toString();
    List<CommunicationWindow> windows = windowsTable.getAll(recipientUuid);

    ZoneId timezone = deriveTimezone(recipient);
    ZonedDateTime now = ZonedDateTime.now(clock.withZone(timezone));

    for (CommunicationWindow window : windows) {
      Optional<CommunicationWindowSchedule> active = window.activeSchedule(now);
      if (active.isPresent()) {
        CommunicationWindowSchedule schedule = active.get();
        return Optional.of(new ActiveWindowInfo(
            window.getName(), schedule.getStart(), schedule.getEnd(), window.getExpectations()));
      }
    }
    return Optional.empty();
  }

  /** Sender-visible snapshot of the recipient's currently-active window (times in recipient tz). */
  public record ActiveWindowInfo(String name, int startMinutes, int endMinutes, WindowExpectations expectations) {}

  // --- Window CRUD ---

  public CommunicationWindow createWindow(String accountUuid, CommunicationWindow window) {
    window.setWindowId(UUID.randomUUID().toString());
    windowsTable.put(accountUuid, window);
    return window;
  }

  public Optional<CommunicationWindow> updateWindow(String accountUuid, String windowId,
      CommunicationWindow updated) {
    // [Smartt] Upsert: the client owns the windowId (local-first, like notification profiles),
    // so a PUT creates the window if it does not exist yet rather than 404ing. This lets the
    // Android client write locally and push in the background with its own client-generated id.
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

  /** Stores a held message and schedules its delivery at the window's opening time. Shared by the
   *  identified-sender and sealed-sender paths; {@code senderAci} is null for sealed sender. */
  private WindowHoldResult holdAndSchedule(String recipientUuid, CommunicationWindow window, ZoneId timezone,
      ServiceIdentifier destinationIdentifier, String senderAci, byte senderDeviceId, IncomingMessageList messages) {

    Instant opensAt = window.windowOpensAt(timezone, clock)
        .orElse(clock.instant().plusSeconds(3600));

    HeldMessageData messageData = buildHeldMessageData(
        destinationIdentifier, senderAci, senderDeviceId, messages);

    String sortKey = heldMessagesTable.store(recipientUuid, opensAt, messageData);

    try {
      deliveryScheduler.scheduleDelivery(recipientUuid, sortKey, opensAt);
    } catch (Exception e) {
      logger.warn("Failed to schedule delivery for held message (recipient={})", recipientUuid, e);
    }

    return new WindowHoldResult(window.getWindowId(), opensAt);
  }

  private HeldMessageData buildHeldMessageData(ServiceIdentifier destination, String senderAci,
      byte senderDeviceId, IncomingMessageList messages) {

    List<HeldIncomingMessage> heldMessages = messages.messages().stream()
        .map(m -> new HeldIncomingMessage(
            m.type(), m.destinationDeviceId(), m.destinationRegistrationId(), m.content()))
        .collect(Collectors.toList());

    long clientTimestamp = messages.timestamp() == 0 ? clock.millis() : messages.timestamp();

    return new HeldMessageData(
        destination.toServiceIdentifierString(),
        senderAci, // null for sealed sender (anonymous)
        senderDeviceId,
        clientTimestamp,
        messages.urgent(),
        false, // isStory: story messages are not held
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
