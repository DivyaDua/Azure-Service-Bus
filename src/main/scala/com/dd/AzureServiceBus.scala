package com.dd

object AzureServiceBus extends App {

  val messageSender = new MessageSender
  val messageReceiver = new MessageReceiver

  messageSender.sendMessage()
  messageSender.sendMessageBatch()
  messageSender.closeSender()

  messageReceiver.receiveMessages()
}
