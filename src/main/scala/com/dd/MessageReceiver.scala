package com.dd

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.azure.messaging.servicebus._
import com.dd.Constants._

class MessageReceiver {

  // handles received messages
  @throws[InterruptedException]
  def receiveMessages(): Unit = {
    val countdownLatch = new CountDownLatch(1)
    // Create an instance of the processor through the ServiceBusClientBuilder
    val processorClient = new ServiceBusClientBuilder()
      .connectionString(ConnectionString)
      .processor
      .queueName(QueueName)
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
    println(s"Error when receiving messages from namespace: ${context.getFullyQualifiedNamespace}. Entity: ${context.getEntityPath}")
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
        println("Unable to sleep for period of time")
    }
    else println(s"Error source ${context.getErrorSource}, reason $reason, message: ${context.getException}")
  }
}
