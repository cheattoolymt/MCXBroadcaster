package com.rtm516.mcxboxbroadcast.core.nethernet;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.kastle.netty.channel.nethernet.signaling.AbstractNetherNetXboxSignaling;
import dev.kastle.netty.channel.nethernet.signaling.NetherNetSignaling;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * A drop-in replacement for {@code dev.kastle.netty.channel.nethernet.signaling.NetherNetXboxRpcSignaling}
 * that fixes the {@link ClassCastException} thrown while processing incoming Xbox signaling frames.
 *
 * <p>The upstream implementation assumes that the {@code params} field of an incoming
 * {@code Signaling_ReceiveMessage_v1_0} JSON-RPC request is always a JSON array of relayed
 * messages and calls {@code json.getAsJsonArray("params")} unconditionally. The
 * {@code signal.franchise.minecraft-services.net} relay however sends {@code params} as a single
 * JSON <em>object</em> for some clients (notably Nintendo Switch), e.g.:
 *
 * <pre>{@code
 * {"jsonrpc":"2.0","id":22,"method":"Signaling_ReceiveMessage_v1_0",
 *  "params":{"From":"...","Message":"...","Id":"..."}}
 * }</pre>
 *
 * <p>Casting that object to a {@link JsonArray} throws
 * {@code java.lang.ClassCastException: JsonObject cannot be cast to JsonArray}, which is swallowed by
 * the frame handler. As a result every {@code CANDIDATEADD} / {@code CONNECTREQUEST} signaling
 * message is dropped, the WebRTC/NetherNet negotiation never completes and the joining Bedrock
 * client fails with the "NetherNet" connection error / "Unable to connect to world".
 *
 * <p>This subclass reimplements the JSON-RPC handling and normalises {@code params} so that both the
 * single-object and the array shape are handled. Everything else mirrors the upstream behaviour.
 *
 * <p>See GeyserMC/Geyser#6543 for the original bug report.
 */
public class NetherNetXboxRpcSignaling extends AbstractNetherNetXboxSignaling {
    private static final String SIGNALING_ENDPOINT =
        "wss://signal.franchise.minecraft-services.net/ws/v1.0/messaging/connect";

    private final Map<String, CompletableFuture<JsonObject>> pendingRequests = new ConcurrentHashMap<>();

    public NetherNetXboxRpcSignaling(String networkId, String xboxToken) {
        super(networkId, xboxToken, URI.create(SIGNALING_ENDPOINT));
    }

    public NetherNetXboxRpcSignaling(long localNetworkId, String xboxToken) {
        this(Long.toUnsignedString(localNetworkId), xboxToken);
    }

    public NetherNetXboxRpcSignaling(String xboxToken) {
        this(Long.toUnsignedString(ThreadLocalRandom.current().nextLong(1L, Long.MAX_VALUE)), xboxToken);
    }

    @Override
    protected void onConnected(ChannelHandlerContext ctx) {
        // Keep the signaling websocket alive.
        ctx.executor().scheduleAtFixedRate(() -> {
            if (this.channel != null && this.channel.isActive()) {
                this.sendJsonRpcRequest("System_Ping_v1_0", new JsonObject());
            }
        }, 30L, 50L, TimeUnit.SECONDS);

        // Fetch the STUN/TURN credentials and complete the connect future with the parsed ICE servers.
        this.sendJsonRpcRequest("Signaling_TurnAuth_v1_0", new JsonObject())
            .thenAccept(response -> {
                List<NetherNetSignaling.IceServerInfo> servers = this.parseTurnServers(response);
                if (this.connectFuture != null && !this.connectFuture.isDone()) {
                    this.connectFuture.complete(servers);
                }
            })
            .exceptionally(t -> {
                this.log.error("Failed to fetch TURN credentials", t);
                if (this.connectFuture != null && !this.connectFuture.isDone()) {
                    this.connectFuture.completeExceptionally(t);
                }
                return null;
            });
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame frame) {
        String text = frame.text();
        try {
            JsonObject json = JsonParser.parseString(text).getAsJsonObject();
            if (json.has("result") || (json.has("error") && json.has("id"))) {
                handleResponse(json);
            } else if (json.has("method")) {
                handleRequest(json);
            }
        } catch (Exception e) {
            this.log.error("Error processing signaling frame: " + text, e);
        }
    }

    private void handleResponse(JsonObject json) {
        if (!json.has("id") || json.get("id").isJsonNull()) {
            return;
        }
        String id = json.get("id").getAsString();
        CompletableFuture<JsonObject> future = this.pendingRequests.remove(id);
        if (future == null) {
            return;
        }

        if (json.has("error") && !json.get("error").isJsonNull()) {
            JsonObject error = json.getAsJsonObject("error");
            String msg = error.has("message") ? error.get("message").getAsString() : error.toString();
            boolean isNotFound = msg.contains("Player not registered");
            if (!isNotFound && error.has("data") && error.get("data").isJsonObject()) {
                JsonObject data = error.getAsJsonObject("data");
                if (data.has("Code") && "MissingOrExpiredIdentity".equals(data.get("Code").getAsString())) {
                    isNotFound = true;
                }
            }
            if (isNotFound && this.notFoundHandler != null) {
                this.notFoundHandler.onNotFound(msg);
            }
            future.completeExceptionally(new RuntimeException(msg));
        } else {
            future.complete(json.has("result") && !json.get("result").isJsonNull()
                ? json.getAsJsonObject("result")
                : new JsonObject());
        }
    }

    private void handleRequest(JsonObject json) {
        String method = json.get("method").getAsString();
        JsonElement id = json.get("id");
        switch (method) {
            case "Signaling_ReceiveMessage_v1_0": {
                if (id != null && !id.isJsonNull()) {
                    sendJsonRpcResult(id, null);
                }

                JsonElement params = json.get("params");
                if (params == null || params.isJsonNull()) {
                    break;
                }

                // The relay may deliver "params" either as a single message object or as an array of
                // message objects. The upstream library only handled the array form and threw a
                // ClassCastException on the object form (breaking Nintendo Switch and other clients).
                if (params.isJsonArray()) {
                    for (JsonElement el : params.getAsJsonArray()) {
                        if (el != null && el.isJsonObject()) {
                            processIncomingMessage(el.getAsJsonObject());
                        }
                    }
                } else if (params.isJsonObject()) {
                    processIncomingMessage(params.getAsJsonObject());
                }
                break;
            }
            case "System_Pong_v1_0":
            case "System_Ping_v1_0": {
                if (id != null && !id.isJsonNull()) {
                    sendJsonRpcResult(id, null);
                }
                break;
            }
            default:
                break;
        }
    }

    private void processIncomingMessage(JsonObject msgObj) {
        String from = msgObj.get("From").getAsString();
        String rawInner = msgObj.get("Message").getAsString();
        String msgId = msgObj.has("Id") ? msgObj.get("Id").getAsString() : UUID.randomUUID().toString();

        // Acknowledge the delivery back to the relay.
        JsonObject innerParams = new JsonObject();
        innerParams.addProperty("messageId", msgId);
        JsonObject innerMsg = new JsonObject();
        innerMsg.add("params", innerParams);
        innerMsg.addProperty("jsonrpc", "2.0");
        innerMsg.addProperty("method", "Signaling_DeliveryNotification_V1_0");
        sendJsonRpcRequest("Signaling_SendClientMessage_v1_0", createSendParams(from, innerMsg.toString()));

        try {
            JsonObject innerJson = JsonParser.parseString(rawInner).getAsJsonObject();
            if (innerJson.has("method") && "Signaling_WebRtc_v1_0".equals(innerJson.get("method").getAsString())) {
                String payload = innerJson.getAsJsonObject("params").get("message").getAsString();
                dispatchSignalToPipeline(from, payload);
            }
        } catch (Exception e) {
            this.log.error("Failed to parse inner signaling message from " + from, e);
        }
    }

    @Override
    public void sendSignal(String targetNetworkId, String data) {
        if (this.channel == null || !this.channel.isActive()) {
            throw new IllegalStateException("Signaling channel is not active");
        }
        JsonObject innerParams = new JsonObject();
        innerParams.addProperty("netherNetId", this.localNetworkId);
        innerParams.addProperty("message", data);
        JsonObject innerMsg = new JsonObject();
        innerMsg.add("params", innerParams);
        innerMsg.addProperty("jsonrpc", "2.0");
        innerMsg.addProperty("method", "Signaling_WebRtc_v1_0");
        sendJsonRpcRequest("Signaling_SendClientMessage_v1_0", createSendParams(targetNetworkId, innerMsg.toString()));
    }

    private JsonObject createSendParams(String toPlayerId, String message) {
        JsonObject params = new JsonObject();
        params.addProperty("toPlayerId", toPlayerId);
        params.addProperty("messageId", UUID.randomUUID().toString());
        params.addProperty("message", message);
        return params;
    }

    private CompletableFuture<JsonObject> sendJsonRpcRequest(String method, JsonObject params) {
        String id = UUID.randomUUID().toString();
        JsonObject rpc = new JsonObject();
        rpc.add("params", params);
        rpc.addProperty("jsonrpc", "2.0");
        rpc.addProperty("method", method);
        rpc.addProperty("id", id);

        CompletableFuture<JsonObject> future = new CompletableFuture<>();
        this.pendingRequests.put(id, future);
        if (this.channel != null && this.channel.isActive()) {
            this.channel.writeAndFlush(new TextWebSocketFrame(rpc.toString()));
        } else {
            this.pendingRequests.remove(id);
            future.completeExceptionally(new ClosedChannelException());
        }
        return future;
    }

    private void sendJsonRpcResult(JsonElement id, JsonElement result) {
        JsonObject response = new JsonObject();
        response.add("id", id);
        response.add("result", result);
        response.addProperty("jsonrpc", "2.0");
        if (this.channel != null && this.channel.isActive()) {
            this.channel.writeAndFlush(new TextWebSocketFrame(response.toString()));
        }
    }
}
