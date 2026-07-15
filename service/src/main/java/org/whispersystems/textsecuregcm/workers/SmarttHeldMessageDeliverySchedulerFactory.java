package org.whispersystems.textsecuregcm.workers;

// [Smartt] Factory for the communication window held-message delivery scheduler.
// Must live in this package because CommandDependencies is package-private.

import com.smarttmessenger.communicationwindow.scheduler.HeldMessageDeliveryScheduler;
import com.smarttmessenger.communicationwindow.storage.HeldMessagesTable;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.metrics.MicrometerAwsSdkMetricPublisher;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.scheduler.JobScheduler;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import java.time.Clock;
import java.util.concurrent.Executors;

public class SmarttHeldMessageDeliverySchedulerFactory implements JobSchedulerFactory {

  @Override
  public JobScheduler buildJobScheduler(final CommandDependencies deps,
      final WhisperServerConfiguration configuration) {

    final DynamoDbClient dynamoDbClient = configuration.getDynamoDbClientConfiguration()
        .buildSyncClient(
            configuration.getAwsCredentialsConfiguration().build(),
            new MicrometerAwsSdkMetricPublisher(
                Executors.newSingleThreadExecutor(), "dynamoDbSmarttCommand"));

    final HeldMessagesTable heldMessagesTable = new HeldMessagesTable(
        dynamoDbClient,
        configuration.getSmarttCommunicationWindow().getHeldMessagesTableName(),
        configuration.getDynamoDbTables().getMessages().getExpiration());

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
