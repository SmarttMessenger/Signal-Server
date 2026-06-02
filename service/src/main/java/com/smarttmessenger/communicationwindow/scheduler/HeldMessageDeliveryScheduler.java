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
  record JobDescriptor(String recipientUuid, long deliverAtMs) {}

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
    final JobDescriptor descriptor;
    try {
      descriptor = SystemMapper.jsonMapper().readValue(jobData, JobDescriptor.class);
    } catch (Exception e) {
      return CompletableFuture.failedFuture(e);
    }

    return accountsManager.getByAccountIdentifierAsync(UUID.fromString(descriptor.recipientUuid()))
        .thenCompose(maybeRecipient -> {
          if (maybeRecipient.isEmpty()) {
            return CompletableFuture.completedFuture("recipientNotFound");
          }

          Account recipient = maybeRecipient.get();
          List<HeldMessagesTable.HeldMessageEntry> entries = heldMessagesTable
              .getMessagesForDelivery(descriptor.recipientUuid(), Instant.ofEpochMilli(descriptor.deliverAtMs()));

          if (entries.isEmpty()) {
            return CompletableFuture.completedFuture("noMessages");
          }

          int delivered = 0;
          for (HeldMessagesTable.HeldMessageEntry entry : entries) {
            try {
              deliverEntry(recipient, entry);
              heldMessagesTable.delete(descriptor.recipientUuid(), entry.sortKey());
              delivered++;
            } catch (Exception e) {
              logger.warn("Failed to deliver held message for recipient={}", descriptor.recipientUuid(), e);
            }
          }

          return CompletableFuture.completedFuture("delivered:" + delivered);
        });
  }

  public CompletableFuture<Void> scheduleDelivery(String recipientUuid, Instant deliverAt) {
    try {
      return scheduleJob(deliverAt, SystemMapper.jsonMapper().writeValueAsBytes(
          new JobDescriptor(recipientUuid, deliverAt.toEpochMilli())));
    } catch (JsonProcessingException e) {
      throw new AssertionError(e);
    }
  }

  private void deliverEntry(Account recipient, HeldMessagesTable.HeldMessageEntry entry)
      throws Exception {

    HeldMessageData data = entry.data();
    ServiceIdentifier destinationId = ServiceIdentifier.valueOf(data.getDestinationServiceId());
    AciServiceIdentifier senderAci = new AciServiceIdentifier(UUID.fromString(data.getSenderAci()));

    Map<Byte, MessageProtos.Envelope> messagesByDeviceId = data.getMessages().stream()
        .collect(Collectors.toMap(
            HeldIncomingMessage::getDestinationDeviceId,
            msg -> MessageProtos.Envelope.newBuilder()
                .setType(MessageProtos.Envelope.Type.forNumber(msg.getType()))
                .setClientTimestamp(data.getClientTimestamp())
                .setServerTimestamp(clock.millis())
                .setDestinationServiceId(destinationId.toServiceIdentifierString())
                .setSourceServiceId(senderAci.toServiceIdentifierString())
                .setSourceDevice(data.getSenderDeviceId())
                .setContent(ByteString.copyFrom(msg.getContent()))
                .setStory(data.isStory())
                .setEphemeral(data.isOnline())
                .setUrgent(data.isUrgent())
                .build()
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
