package com.smarttmessenger.communicationwindow.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.smarttmessenger.communicationwindow.storage.HeldMessagesTable;
import com.smarttmessenger.communicationwindow.model.HeldIncomingMessage;
import com.smarttmessenger.communicationwindow.model.HeldMessageData;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
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
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import javax.annotation.Nullable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class HeldMessageDeliveryScheduler extends JobScheduler {

  private static final Logger logger = LoggerFactory.getLogger(HeldMessageDeliveryScheduler.class);

  private final AccountsManager accountsManager;
  private final MessageSender messageSender;
  private final HeldMessagesTable heldMessagesTable;
  private final Clock clock;

  @VisibleForTesting
  record JobDescriptor(String recipientUuid, String sortKey) {}

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

  @Override
  protected CompletableFuture<String> processJob(@Nullable byte[] jobData) {
    final JobDescriptor descriptor = parseJobDescriptor(jobData);

    // Malformed or legacy pre-sortKey payloads can never succeed; complete the job so it gets
    // deleted instead of being retried on every sweep until its TTL.
    if (descriptor == null
        || descriptor.recipientUuid() == null || descriptor.recipientUuid().isBlank()
        || descriptor.sortKey() == null || descriptor.sortKey().isBlank()) {
      logger.warn("Discarding malformed held-message delivery job: {}",
          jobData == null ? "null" : new String(jobData, java.nio.charset.StandardCharsets.UTF_8));
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

    return accountsManager.getByAccountIdentifierAsync(recipientUuid)
        .thenApply(maybeRecipient -> {
          if (maybeRecipient.isEmpty()) {
            return "recipientNotFound";
          }

          // Exactly one job per held message, keyed by sort key — no draining, so concurrent
          // jobs can never deliver the same message twice.
          Optional<HeldMessagesTable.HeldMessageEntry> maybeEntry =
              heldMessagesTable.getEntry(descriptor.recipientUuid(), descriptor.sortKey());

          if (maybeEntry.isEmpty()) {
            return "noMessage";
          }

          try {
            deliverEntry(maybeRecipient.get(), maybeEntry.get());
            heldMessagesTable.delete(descriptor.recipientUuid(), descriptor.sortKey());
            return "delivered";
          } catch (Exception e) {
            // Throw so the job is NOT deleted and is retried on the next sweep (entry stays put).
            logger.warn("Failed to deliver held message for recipient={}", descriptor.recipientUuid(), e);
            throw new RuntimeException(e);
          }
        });
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

  public CompletableFuture<Void> scheduleDelivery(String recipientUuid, String sortKey, Instant deliverAt) {
    try {
      return scheduleJob(deliverAt, SystemMapper.jsonMapper().writeValueAsBytes(
          new JobDescriptor(recipientUuid, sortKey)));
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
