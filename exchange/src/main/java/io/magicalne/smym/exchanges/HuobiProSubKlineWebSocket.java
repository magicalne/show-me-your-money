package io.magicalne.smym.exchanges;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import io.magicalne.smym.Utils;
import io.magicalne.smym.handler.SubEventHandler;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import okio.ByteString;

import java.io.IOException;
import java.net.Proxy;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Slf4j
public class HuobiProSubKlineWebSocket extends WebSocketListener {

    private final Set<String> symbols;
    private final String period;
    private final SubEventHandler subEventHandler;
    private final ObjectMapper objectMapper;

    private static final String KLINE_FORMAT = "{\"sub\": \"market.%s.kline.%s\", \"id\": \"%s\"}";

    public HuobiProSubKlineWebSocket(Set<String> symbols, String period, SubEventHandler subEventHandler) {
        this.symbols = symbols;
        this.period = period;
        this.subEventHandler = subEventHandler;
        this.objectMapper = new ObjectMapper();
    }

    public void init() {
        OkHttpClient client;
        String httpProxy = System.getenv("http_proxy");
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        if (!Strings.isNullOrEmpty(httpProxy)) {
            Proxy proxy = Utils.getProxyFromEnv(httpProxy);
            builder = builder.proxy(proxy);
        }
        client = builder.readTimeout(0, TimeUnit.MILLISECONDS).build();
        Request request = new Request.Builder()
                .url("wss://api.huobi.pro/ws")
                .build();
        WebSocket webSocket = client.newWebSocket(request, this);

        if (symbols != null && !symbols.isEmpty()) {
            for (String symbol : symbols) {
                String sub = String.format(KLINE_FORMAT, symbol, period, "sub:"+symbol);
                webSocket.send(sub);
            }
            log.info("Subscribe symbols done.");
        }
        // Trigger shutdown of the dispatcher's executor so this process can exit cleanly.
        client.dispatcher().executorService().shutdown();
    }

    @Override
    public void onOpen(WebSocket webSocket, Response response) {
//        webSocket.send("Hello...");
//        webSocket.send("...World!");
//        webSocket.send(ByteString.decodeHex("deadbeef"));
//        webSocket.close(1000, "Goodbye, World!");
    }

    @Override
    public void onMessage(WebSocket webSocket, String text) {
        System.out.println(webSocket.request());
        System.out.println("MESSAGE: " + text);

    }

    @Override
    public void onMessage(WebSocket webSocket, ByteString bytes) {
        try {
            byte[] unGzip = Utils.ungzip(bytes.toByteArray());
            String res = new String(unGzip);
            String ping = "ping";
            if (res.contains(ping)) {
                webSocket.send(res.replace(ping, "pong"));
            } else {
                Map<String, Object> json = objectMapper.readValue(res, new TypeReference<Map<String, Object>>(){});
                Object ch = json.get("ch");
                if (ch != null && ch.toString().startsWith("market")) {
                    subEventHandler.update(res);
                }
            }
        } catch (IOException e) {
            log.error("Read message with exception.", e);
        } catch (Exception e) {
            log.error("WTF???", e);
        }
    }

    @Override
    public void onClosing(WebSocket webSocket, int code, String reason) {
        webSocket.close(1000, null);
        log.info("CLOSE: " + code + " " + reason);
    }

    @Override
    public void onFailure(WebSocket webSocket, Throwable t, Response response) {
        log.error("Fucking fuck fucked. throwable: {}, response: {}", t, response);
        webSocket.close(1003, "Cannot handle message.");
        init();
        log.info("Reopen websocket connection to server.");
    }

}
