import com.azure.messaging.servicebus._
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import util.control.Breaks._

object AzureServiceBusTest extends App {

    val connectionString = "Endpoint=sb://dd-test-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=5MSHpsr/qPRU6krhXa5AM5HT6kAow5uAEAha6sObEYo="
    val queueName = "dd-test-queue"

    private def sendMessage(): Unit = { // create a Service Bus Sender client for the queue
      val senderClient = new ServiceBusClientBuilder().connectionString(connectionString).sender.queueName(queueName).buildClient
      // send one message to the queue
      senderClient.sendMessage(new ServiceBusMessage("Hello, World!"))
      println("Sent a single message to the queue: " + queueName)
    }

    private def createMessages: List[ServiceBusMessage] = { // create a list of messages and return it to the caller
      List(
        new ServiceBusMessage("First message"),
        new ServiceBusMessage("Second message"),
        new ServiceBusMessage("Third message"),
        new ServiceBusMessage("Fourth message"),
        new ServiceBusMessage("Fifth message"),
      )
    }

    private def sendMessageBatch(): Unit = {
      val senderClient = new ServiceBusClientBuilder().connectionString(connectionString).sender.queueName(queueName).buildClient
      // Creates an ServiceBusMessageBatch where the ServiceBus.
      var messageBatch = senderClient.createMessageBatch
      // create a list of messages
      val listOfMessages = createMessages
      
      for (message <- listOfMessages) {
        breakable {
          if (messageBatch.tryAddMessage(message))
            break
          else {
            senderClient.sendMessages(messageBatch)
            println("Sent a batch of messages to the queue: " + queueName)

            messageBatch = senderClient.createMessageBatch

            if (!messageBatch.tryAddMessage(message)) 
              println("Message is too large for an empty batch. Skipping. Max size: %s.", messageBatch.getMaxSizeInBytes)
          }

        } 
      }
      if (messageBatch.getCount > 0) {
        senderClient.sendMessages(messageBatch)
        println("Sent a batch of messages to the queue: " + queueName)
      }
      //close the client
      senderClient.close()
    }

    // handles received messages
    @throws[InterruptedException]
    private def receiveMessages(): Unit = {
      val countdownLatch = new CountDownLatch(1)
      // Create an instance of the processor through the ServiceBusClientBuilder
      val processorClient = new ServiceBusClientBuilder()
        .connectionString(connectionString)
        .processor
        .queueName(queueName)
        .processMessage(processMessage)
        .processError((context: ServiceBusErrorContext) => processError(context, countdownLatch))
        .buildProcessorClient

      println("Starting the processor")
      processorClient.start()

      TimeUnit.SECONDS.sleep(10)

      println("Stopping and closing the processor")
      processorClient.close()
    }

    private def processMessage(context: ServiceBusReceivedMessageContext): Unit = {
      val message = context.getMessage
      println(s"Processing message. Session: ${message.getMessageId}, Sequence #: ${message.getSequenceNumber}. Contents: ${message.getBody}")
    }

    private def processError(context: ServiceBusErrorContext, countdownLatch: CountDownLatch): Unit = {
      println(s"Error when receiving messages from namespace: '${context.getFullyQualifiedNamespace}'. Entity: ${context.getEntityPath}")
      if (!context.getException.isInstanceOf[ServiceBusException]) {
        println(s"Non-ServiceBusException occurred: ${context.getException}")
        return
      }
      val exception = context.getException.asInstanceOf[ServiceBusException]
      val reason = exception.getReason
      if ((reason eq ServiceBusFailureReason.MESSAGING_ENTITY_DISABLED) || (reason eq ServiceBusFailureReason.MESSAGING_ENTITY_NOT_FOUND) || (reason eq ServiceBusFailureReason.UNAUTHORIZED)) {
        println(s"An unrecoverable error occurred. Stopping processing with reason $reason: ${exception.getMessage}")
        countdownLatch.countDown()
      }
      else if (reason eq ServiceBusFailureReason.MESSAGE_LOCK_LOST) println(s"Message lock lost for message: ${context.getException}")
      else if (reason eq ServiceBusFailureReason.SERVICE_BUSY) try // Choosing an arbitrary amount of time to wait until trying again.
        TimeUnit.SECONDS.sleep(1)
      catch {
        case _: InterruptedException =>
          System.err.println("Unable to sleep for period of time")
      }
      else println(s"Error source ${context.getErrorSource}, reason $reason, message: ${context.getException}")
    }

    sendMessage()
    sendMessageBatch()
    receiveMessages()
}
