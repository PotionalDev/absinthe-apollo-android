package com.potional.absinthechannelssubscriptiontransport.library

import android.util.Log
import com.apollographql.apollo.subscription.OperationClientMessage
import com.apollographql.apollo.subscription.OperationServerMessage
import com.apollographql.apollo.subscription.SubscriptionTransport
import org.phoenixframework.PhxMessage
import org.phoenixframework.PhxSocket

private const val ABSINTHE_CHANNEL_TOPIC = "__absinthe__:control"

class AbsintheChannelsSubscriptionTransport(
    webSocketUrl: String,
    private val callback: SubscriptionTransport.Callback,
    params: Map<String, Any>
) : SubscriptionTransport {

    val socket = PhxSocket(webSocketUrl, params)
    val channel = socket.channel(ABSINTHE_CHANNEL_TOPIC)
    private val activeSubscriptions = mutableMapOf<String, String>()

    override fun disconnect(message: OperationClientMessage?) {
        channel.leave()
        socket.disconnect()
    }

    override fun connect() {
        socket.connect()
        socket.onOpen(this::onSocketOpen)
        socket.onMessage(this::onAbsintheMessage)
    }

    private fun onAbsintheMessage(message: PhxMessage) {
        val subscriptionId = getSubscriptionApolloId(message.topic)
        if (subscriptionId != null) {
            callback.onMessage(absintheMessageToApolloData(subscriptionId, message))
        }
    }

    private fun absintheMessageToApolloData(subscriptionId: String, message: PhxMessage): OperationServerMessage.Data {
        return OperationServerMessage.Data(subscriptionId, message.payload["result"] as Map<String, Any>)
    }

    private fun onSocketOpen() {
        channel
            .join()
            .receive("ok") {
                emitApolloAck()
            }
    }

    private fun emitApolloAck() {
        callback.onMessage(OperationServerMessage.ConnectionAcknowledge())
    }

    private fun getSubscriptionApolloId(absintheSubscriptionId: String): String? {
        return activeSubscriptions[absintheSubscriptionId]
    }

    override fun send(message: OperationClientMessage?) {
        when (message) {
            is OperationClientMessage.Init -> Log.d("INIT", "INIT__________________")
            is OperationClientMessage.Start -> sendSubscriptionStartMessage(message)
            is OperationClientMessage.Stop -> sendUnsubscriptionStopMessage(message)
            is OperationClientMessage.Terminate -> disconnect(message)
        }
    }

    private fun sendSubscriptionStartMessage(message: OperationClientMessage.Start) {
        channel
            .push("doc", apolloMessageToAbsintheData(message))
            .receive("ok") {
                val subscriptionId = (it.payload["response"] as Map<String, Any>)["subscriptionId"] as String
                activeSubscriptions[subscriptionId] = message.subscriptionId
            }
    }

    private fun sendUnsubscriptionStopMessage(message: OperationClientMessage.Stop) {
        val subscriptionId = getSubscriptionApolloId(message.subscriptionId)
        if (subscriptionId != null) {
            channel.push("unsubscribe", mapOf(
                "subscriptionId" to activeSubscriptions[subscriptionId] as Any
            )).receive("stop") {
                //TODO
            }
        }
    }

    private fun apolloMessageToAbsintheData(message: OperationClientMessage.Start): Map<String, Any> {
        return mapOf(
            "query" to message.subscription.queryDocument().replace("\\n".toRegex(), "")
        )
    }

    private fun unsubscribe(message: OperationClientMessage.Stop) {
        activeSubscriptions.remove(message.subscriptionId)
    }

    class Factory(
        private val webSocketUrl: String,
        private val params: Map<String, Any> = mapOf()
    ): SubscriptionTransport.Factory {

        override fun create(callback: SubscriptionTransport.Callback): SubscriptionTransport {
            return AbsintheChannelsSubscriptionTransport(webSocketUrl, callback, params)
        }
    }
}