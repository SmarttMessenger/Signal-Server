package com.smarttmessenger.communicationwindow.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.smarttmessenger.communicationwindow.storage.HeldMessagesTable;
import com.smarttmessenger.communicationwindow.model.HeldIncomingMessage;
import com.smarttmessenger.communicationwindow.model.HeldMessageData;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.push.MessageTooLargeException;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.scheduler.JobScheduler;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import javax.annotation.Nullable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class HeldMessageDeliveryScheduler extends JobScheduler {

  private static final Logger logger = LoggerFactory.getLogger(HeldMessageDeliveryScheduler.class);

  /**
   * Bounds how long one job can hold a worker slot. Anything left over is picked up by an immediately
   * rescheduled job, still in order, so a pathological backlog can't monopolise the sweep.
   */
  private static final int MAX_DRAIN_PER_JOB = 500;

  /** Long enough for a full {@link #MAX_DRAIN_PER_JOB} drain; a dead holder self-heals after it. */
  private static final Duration DRAIN_LEASE = Duration.ofMinutes(2);

  private final AccountsManager accountsManager;
  private final MessageSender messageSender;
  private final HeldMessagesTable heldMessagesTable;
  private final Clock clock;

  /**
   * Per-sweep tallies, reset when {@link #processAvailableJobs()} is subscribed and reported in its
   * summary line. Safe as plain instance state because a sweep never overlaps itself: the worker's
   * {@code ScheduledJobProcessor} blocks each tick on a latch until the sweep terminates.
   */
  private final AtomicInteger sweepJobs = new AtomicInteger();
  private final AtomicInteger sweepDelivered = new AtomicInteger();
  private final AtomicInteger sweepDiscarded = new AtomicInteger();

  /** Keeps a corrupt row from flooding the log when its payload is dumped. */
  private static final int MAX_LOGGED_JOB_DATA_CHARS = 256;

  /**
   * One job per window opening. {@code sortKey} is the legacy per-message form, still handled so jobs
   * scheduled before this change keep delivering; {@code deliverAtMs} is the current form.
   */
  @VisibleForTesting
  record JobDescriptor(String recipientUuid, String sortKey, Long deliverAtMs) {}

  public HeldMessageDeliveryScheduler(AccountsManager accountsManager,
      MessageSender messageSender,
      HeldMessagesTable heldMessagesTable,
      DynamoDbAsyncClient dynamoDbAsyncClient,
      String tableName,
      Duration jobExpiration,
      Clock clock) {
    super(dynamoDbAsyncClient, tableName, jobExpiration, clock);
    this.accountsManager = accountsManager;
    this.messageSender = messageSender;
    this.heldMessagesTable = heldMessagesTable;
    this.clock = clock;
  }

  @Override
  public String getSchedulerName() {
    return "SmarttCommunicationWindowDelivery";
  }

  /**
   * Wraps the inherited sweep with a summary line. One line is emitted per sweep even when nothing was
   * due, so a silent log means the worker process itself is not running — otherwise a stopped worker
   * and an idle one are indistinguishable.
   */
  @Override
  public Mono<Void> processAvailableJobs() {
    return Mono.defer(() -> {
      // Reset on subscription, not assembly, so the counters belong to this run of the sweep.
      final long startedAtMs = clock.millis();
      sweepJobs.set(0);
      sweepDelivered.set(0);
      sweepDiscarded.set(0);

      return super.processAvailableJobs()
          .doOnSuccess(ignored -> logger.info(
              "Held-message sweep finished: jobs={} delivered={} discarded={} durationMs={}",
              sweepJobs.get(), sweepDelivered.get(), sweepDiscarded.get(), clock.millis() - startedAtMs))
          .doOnError(e -> logger.warn(
              "Held-message sweep failed: jobs={} delivered={} discarded={} durationMs={}",
              sweepJobs.get(), sweepDelivered.get(), sweepDiscarded.get(), clock.millis() - startedAtMs, e));
    });
  }

  @Override
  protected CompletableFuture<String> processJob(@Nullable byte[] jobData) {
    sweepJobs.incrementAndGet();

    final JobDescriptor descriptor = parseJobDescriptor(jobData);

    // Malformed payloads can never succeed; complete the job so it gets deleted instead of being
    // retried on every sweep until its TTL. A payload needs a recipient plus either form of target.
    final boolean hasTarget = descriptor != null
        && (descriptor.deliverAtMs() != null
            || (descriptor.sortKey() != null && !descriptor.sortKey().isBlank()));
    if (descriptor == null
        || descriptor.recipientUuid() == null || descriptor.recipientUuid().isBlank()
        || !hasTarget) {
      logger.warn("Discarding malformed held-message delivery job: {}", describeJobData(jobData));
      return CompletableFuture.completedFuture("malformedJob");
    }

    final UUID recipientUuid;
    try {
      recipientUuid = UUID.fromString(descriptor.recipientUuid());
    } catch (IllegalArgumentException e) {
      logger.warn("Discarding held-message delivery job with invalid recipient UUID: {}",
          descriptor.recipientUuid());
      return CompletableFuture.completedFuture("malformedJob");
    }

    logger.debug("Processing held-message delivery job: recipient={} deliverAt={} legacySortKey={}",
        descriptor.recipientUuid(), descriptor.deliverAtMs(), descriptor.sortKey());

    return accountsManager.getByAccountIdentifierAsync(recipientUuid)
        .thenApply(maybeRecipient -> {
          if (maybeRecipient.isEmpty()) {
            // The account was deleted while its messages were held; the job can never succeed.
            logger.warn("Discarding held-message delivery job for unknown recipient={} (deliverAt={})",
                descriptor.recipientUuid(), descriptor.deliverAtMs());
            return "recipientNotFound";
          }

          return descriptor.deliverAtMs() != null
              ? drain(maybeRecipient.get(), descriptor.recipientUuid(), descriptor.deliverAtMs())
              : deliverSingle(maybeRecipient.get(), descriptor.recipientUuid(), descriptor.sortKey());
        });
  }

  /**
   * Delivers the recipient's whole due backlog, oldest arrival first, one message at a time.
   *
   * <p>Sequential delivery is the point: {@code MessagesManager.insert} assigns each message its
   * queue position as it is inserted, so concurrent delivery of a backlog produces an arbitrary
   * order. The per-recipient lease makes this the only drain running for the recipient.
   */
  private String drain(final Account recipient, final String recipientUuid, final long deliverAtMs) {
    final long startedAtMs = clock.millis();
    final long leaseExpiry = startedAtMs + DRAIN_LEASE.toMillis();

    if (!heldMessagesTable.tryAcquireDrainLease(recipientUuid, clock.millis(), leaseExpiry)) {
      final int stillDue = heldMessagesTable.getDueEntries(recipientUuid, clock.millis()).size();
      if (stillDue == 0) {
        // A concurrent drain already delivered everything; complete so this job is deleted.
        logger.debug("Held messages already drained by a concurrent job: recipient={} deliverAt={}",
            recipientUuid, deliverAtMs);
        return "alreadyDrained";
      }
      // Work remains but someone else owns the drain. Fail so this job is retried rather than
      // deleted — that is what guarantees the backlog is never abandoned. Logged here because the
      // throw surfaces upstream as an untagged "Failed to process job" with no recipient.
      logger.warn("Drain already in progress: recipient={} deliverAt={} due={}",
          recipientUuid, deliverAtMs, stillDue);
      throw new IllegalStateException("drain already in progress for recipient=" + recipientUuid);
    }

    // Queried once and reused for the loop; the "anything left?" check after the loop must be a fresh
    // query because messages can arrive mid-drain.
    final List<HeldMessagesTable.HeldMessageEntry> dueEntries =
        heldMessagesTable.getDueEntries(recipientUuid, clock.millis());

    // How far past the window opening this release actually is — the headline number for "are held
    // messages going out on time?".
    final long lateByMs = startedAtMs - deliverAtMs;

    logger.info("Draining held messages: recipient={} deliverAt={} due={} lateByMs={}",
        recipientUuid, deliverAtMs, dueEntries.size(), lateByMs);

    int delivered = 0;
    int discarded = 0;
    try {
      for (HeldMessagesTable.HeldMessageEntry entry : dueEntries) {
        if (delivered + discarded >= MAX_DRAIN_PER_JOB) {
          break;
        }
        try {
          deliverEntry(recipient, entry);
          delivered++;
          logger.debug("Delivered held message: recipient={} sortKey={} devices={}",
              recipientUuid, entry.sortKey(), entry.data().getMessages().size());
        } catch (MismatchedDevicesException | MessageTooLargeException e) {
          // Permanently undeliverable: the recipient's devices changed while the message was held, or
          // it is simply too large. Retrying can never succeed, and because a drain is sequential, a
          // message we kept retrying would block every message behind it until the job's TTL. Drop it
          // and carry on, in the same spirit as discarding malformed jobs.
          logger.warn("Discarding undeliverable held message for recipient={} (sortKey={})",
              recipientUuid, entry.sortKey(), e);
          discarded++;
        }
        heldMessagesTable.delete(recipientUuid, entry.sortKey());
      }
    } catch (Exception e) {
      // Retried on the next sweep, resuming at the first undelivered entry since delivered ones are
      // already gone — so a mid-backlog failure keeps the ordering intact.
      heldMessagesTable.releaseDrainLease(recipientUuid, leaseExpiry);
      recordSweepCounts(delivered, discarded);
      logger.warn("Failed to drain held messages: recipient={} deliverAt={} due={} delivered={} "
              + "discarded={} durationMs={} lateByMs={}",
          recipientUuid, deliverAtMs, dueEntries.size(), delivered, discarded,
          clock.millis() - startedAtMs, lateByMs, e);
      throw new RuntimeException(e);
    }

    recordSweepCounts(delivered, discarded);

    // Anything left means the cap was hit or a message arrived mid-drain: keep the marker and run
    // again rather than dropping the remainder.
    // Outcomes are used as metric tag values, so they must come from a small fixed set — counts go to
    // the log, never into the returned status.
    final int remaining = heldMessagesTable.getDueEntries(recipientUuid, clock.millis()).size();
    if (remaining > 0) {
      heldMessagesTable.releaseDrainLease(recipientUuid, leaseExpiry);
      scheduleDrain(recipientUuid, deliverAtMs, clock.instant());
      logger.info("Partially drained held messages: recipient={} deliverAt={} due={} delivered={} "
              + "discarded={} remaining={} cappedAtMaxPerJob={} durationMs={} lateByMs={}",
          recipientUuid, deliverAtMs, dueEntries.size(), delivered, discarded, remaining,
          delivered + discarded >= MAX_DRAIN_PER_JOB, clock.millis() - startedAtMs, lateByMs);
      return "partialDrain";
    }

    heldMessagesTable.deleteDrainMarker(recipientUuid, deliverAtMs);
    heldMessagesTable.releaseDrainLease(recipientUuid, leaseExpiry);
    logger.info("Drained held messages: recipient={} deliverAt={} due={} delivered={} discarded={} "
            + "durationMs={} lateByMs={}",
        recipientUuid, deliverAtMs, dueEntries.size(), delivered, discarded,
        clock.millis() - startedAtMs, lateByMs);
    return discarded == 0 ? "drained" : "drainedWithDiscards";
  }

  private void recordSweepCounts(final int delivered, final int discarded) {
    sweepDelivered.addAndGet(delivered);
    sweepDiscarded.addAndGet(discarded);
  }

  /** Legacy path for per-message jobs scheduled before delivery became a drain. */
  private String deliverSingle(final Account recipient, final String recipientUuid, final String sortKey) {
    // Logged at INFO despite being low volume: it is the only way to tell whether pre-drain jobs are
    // still present in production.
    logger.info("Delivering legacy per-message held job: recipient={} sortKey={}", recipientUuid, sortKey);

    final Optional<HeldMessagesTable.HeldMessageEntry> maybeEntry =
        heldMessagesTable.getEntry(recipientUuid, sortKey);

    if (maybeEntry.isEmpty()) {
      logger.info("No held message found for legacy job: recipient={} sortKey={}", recipientUuid, sortKey);
      return "noMessage";
    }

    try {
      deliverEntry(recipient, maybeEntry.get());
      heldMessagesTable.delete(recipientUuid, sortKey);
      recordSweepCounts(1, 0);
      logger.info("Delivered legacy held message: recipient={} sortKey={}", recipientUuid, sortKey);
      return "delivered";
    } catch (Exception e) {
      // Throw so the job is NOT deleted and is retried on the next sweep (entry stays put).
      logger.warn("Failed to deliver held message: recipient={} sortKey={}", recipientUuid, sortKey, e);
      throw new RuntimeException(e);
    }
  }

  /** Truncated, null-safe rendering of a job payload for log messages. */
  private static String describeJobData(@Nullable final byte[] jobData) {
    if (jobData == null) {
      return "null";
    }
    final String rendered = new String(jobData, java.nio.charset.StandardCharsets.UTF_8);
    return rendered.length() <= MAX_LOGGED_JOB_DATA_CHARS
        ? rendered
        : rendered.substring(0, MAX_LOGGED_JOB_DATA_CHARS) + "...(truncated, " + rendered.length() + " chars)";
  }

  @Nullable
  private static JobDescriptor parseJobDescriptor(@Nullable final byte[] jobData) {
    if (jobData == null) {
      return null;
    }
    try {
      return SystemMapper.jsonMapper().readValue(jobData, JobDescriptor.class);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Schedules the single drain that releases everything held for this recipient's window opening.
   * Callers must have won {@code tryCreateDrainMarker} first — that is what keeps this to one job per
   * window opening instead of one per message, and keeps the shared jobs partition off the critical
   * path of a large backlog.
   */
  public CompletableFuture<Void> scheduleDrain(String recipientUuid, long deliverAtMs, Instant runAt) {
    try {
      return scheduleJob(runAt, SystemMapper.jsonMapper().writeValueAsBytes(
          new JobDescriptor(recipientUuid, null, deliverAtMs)));
    } catch (JsonProcessingException e) {
      throw new AssertionError(e);
    }
  }

  private void deliverEntry(Account recipient, HeldMessagesTable.HeldMessageEntry entry)
      throws Exception {

    HeldMessageData data = entry.data();
    ServiceIdentifier destinationId = ServiceIdentifier.valueOf(data.getDestinationServiceId());
    final String senderAciString = data.getSenderAci();

    Map<Byte, MessageProtos.Envelope> messagesByDeviceId = data.getMessages().stream()
        .collect(Collectors.toMap(
            HeldIncomingMessage::getDestinationDeviceId,
            msg -> {
              MessageProtos.Envelope.Builder builder = MessageProtos.Envelope.newBuilder()
                  .setType(MessageProtos.Envelope.Type.forNumber(msg.getType()))
                  .setClientTimestamp(data.getClientTimestamp())
                  .setServerTimestamp(clock.millis())
                  .setDestinationServiceId(destinationId.toServiceIdentifierString())
                  .setContent(ByteString.copyFrom(msg.getContent()))
                  .setStory(data.isStory())
                  .setEphemeral(data.isOnline())
                  .setUrgent(data.isUrgent());
              // Identified sender: set the source. Sealed sender (senderAci null) stays anonymous.
              if (senderAciString != null && !senderAciString.isBlank()) {
                AciServiceIdentifier senderAci = new AciServiceIdentifier(UUID.fromString(senderAciString));
                builder.setSourceServiceId(senderAci.toServiceIdentifierString())
                    .setSourceDevice(data.getSenderDeviceId());
              }
              return builder.build();
            }
        ));

    Map<Byte, Integer> registrationIdsByDeviceId = data.getMessages().stream()
        .collect(Collectors.toMap(
            HeldIncomingMessage::getDestinationDeviceId,
            HeldIncomingMessage::getDestinationRegistrationId
        ));

    messageSender.sendMessages(recipient, destinationId, messagesByDeviceId,
        registrationIdsByDeviceId, Optional.empty(), null);
  }
}
