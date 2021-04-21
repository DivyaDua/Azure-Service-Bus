package com.dd

import com.dd.Constants._
import com.azure.messaging.servicebus.{ServiceBusClientBuilder, ServiceBusMessage, ServiceBusSenderClient}

import scala.util.control.Breaks.{break, breakable}

class MessageSender {

  // create a Service Bus Sender client for the queue
  val senderClient: ServiceBusSenderClient = new ServiceBusClientBuilder()
    .connectionString(ConnectionString)
    .sender
    .queueName(QueueName)
    .buildClient

  private def createMessages(): List[ServiceBusMessage] = {
    // create a list of messages and return it to the caller
    List(
      new ServiceBusMessage("First message"),
      new ServiceBusMessage("Second message"),
      new ServiceBusMessage("Third message"),
      new ServiceBusMessage("Fourth message"),
      new ServiceBusMessage("Fifth message"),
    )
  }

  def sendMessageBatch(): Unit = {
    // Creates an ServiceBusMessageBatch where the ServiceBus.
    var messageBatch = senderClient.createMessageBatch
    // create a list of messages
    val listOfMessages = createMessages()

    for (message <- listOfMessages) {
      breakable {
        if (messageBatch.tryAddMessage(message))
          break
        else {
          senderClient.sendMessages(messageBatch)
          println("Sent a batch of messages to the queue: " + QueueName)

          messageBatch = senderClient.createMessageBatch
          if (!messageBatch.tryAddMessage(message))
            println(s"Message is too large for an empty batch. Skipping. Max size: ${messageBatch.getMaxSizeInBytes}")
        }
      }
    }

    if (messageBatch.getCount > 0) {
      senderClient.sendMessages(messageBatch)
      println(s"Sent a batch of messages to the queue: $QueueName")
    }
  }

  def sendMessage(): Unit = {
    // send one message to the queue
    senderClient.sendMessage(new ServiceBusMessage("Hello, World!"))
    println(s"Sent a single message to the queue: $QueueName")
  }

  def closeSender(): Unit = senderClient.close()
}
