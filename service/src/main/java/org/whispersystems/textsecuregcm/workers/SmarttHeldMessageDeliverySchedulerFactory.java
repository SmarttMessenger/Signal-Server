package org.whispersystems.textsecuregcm.workers;

// [Smartt] Factory for the communication window held-message delivery scheduler.
// Must live in this package because CommandDependencies is package-private.

import com.smarttmessenger.communicationwindow.scheduler.HeldMessageDeliveryScheduler;
import com.smarttmessenger.communicationwindow.storage.HeldMessagesTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.metrics.MicrometerAwsSdkMetricPublisher;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.scheduler.JobScheduler;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import java.time.Clock;
import java.util.concurrent.Executors;

public class SmarttHeldMessageDeliverySchedulerFactory implements JobSchedulerFactory {

  private static final Logger logger = LoggerFactory.getLogger(SmarttHeldMessageDeliverySchedulerFactory.class);

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

    // The resolved table names are worth a line: config.yml carries no `smarttCommunicationWindow`
    // block, so these come from defaults, and a wrong table looks exactly like "the worker runs but
    // never finds anything".
    logger.info("Starting held-message delivery scheduler: heldMessagesTable={} scheduledJobsTable={} "
            + "heldMessageExpiration={} jobExpiration={}",
        configuration.getSmarttCommunicationWindow().getHeldMessagesTableName(),
        configuration.getDynamoDbTables().getScheduledJobs().getTableName(),
        configuration.getDynamoDbTables().getMessages().getExpiration(),
        configuration.getDynamoDbTables().getScheduledJobs().getExpiration());

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
