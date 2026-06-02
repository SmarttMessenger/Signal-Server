package org.whispersystems.textsecuregcm.workers;

// [Smartt] Factory for the communication window held-message delivery scheduler.
// Must live in this package because CommandDependencies is package-private.

import com.smarttmessenger.communicationwindow.scheduler.HeldMessageDeliveryScheduler;
import com.smarttmessenger.communicationwindow.storage.HeldMessagesTable;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.scheduler.JobScheduler;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import java.time.Clock;

public class SmarttHeldMessageDeliverySchedulerFactory implements JobSchedulerFactory {

  @Override
  public JobScheduler buildJobScheduler(final CommandDependencies deps,
      final WhisperServerConfiguration configuration) {

    final DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
        .region(Region.of(configuration.getDynamoDbClientConfiguration().getRegion()))
        .credentialsProvider(configuration.getAwsCredentialsConfiguration().build())
        .build();

    final HeldMessagesTable heldMessagesTable = new HeldMessagesTable(
        dynamoDbClient,
        configuration.getSmarttCommunicationWindow().getHeldMessagesTableName());

    final MessageSender messageSender = new MessageSender(
        deps.messagesManager(),
        deps.pushNotificationManager());

    return new HeldMessageDeliveryScheduler(
        deps.accountsManager(),
        messageSender,
        heldMessagesTable,
        deps.dynamoDbAsyncClient(),
        configuration.getDynamoDbTables().getScheduledJobs().getTableName(),
        configuration.getDynamoDbTables().getScheduledJobs().getExpiration(),
        Clock.systemUTC());
  }
}
