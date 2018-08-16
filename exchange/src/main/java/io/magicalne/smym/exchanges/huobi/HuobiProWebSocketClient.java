package io.magicalne.smym.exchanges.huobi;

import com.binance.api.client.Util;
import io.magicalne.smym.dto.DepthResponse;
import io.magicalne.smym.exchanges.UniverseApiCallback;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.WebSocket;

import java.io.Closeable;
import java.util.Set;

public class HuobiProWebSocketClient implements Closeable {

    private static final String API_HUOBI_PRO_WS = "wss://api.huobi.pro/ws";
    private static final String SUB_REQUEST_TEMPLATE = "{\"sub\": \"%s\", \"id\": \"%s\"}";
    private OkHttpClient client;

    public HuobiProWebSocketClient() {
        Dispatcher d = new Dispatcher();
        d.setMaxRequestsPerHost(200);
        this.client = Util.createOKHTTPClient().dispatcher(d).build();
    }

    Closeable createNewWebSocket(String template, Set<String> symbols, HuobiApiWebSocketListener<?> listener) {
        Request request = new Request.Builder().url(API_HUOBI_PRO_WS).build();
        final WebSocket webSocket = client.newWebSocket(request, listener);
        for (String symbol : symbols) {
            String topic = String.format(template, symbol);
            String reqBody = String.format(SUB_REQUEST_TEMPLATE, topic, symbol);
            webSocket.send(reqBody);
        }
        return () -> {
            final int code = 1000;
            listener.onClosing(webSocket, code, null);
            webSocket.close(code, null);
            listener.onClosed(webSocket, code, null);
        };
    }

    public Closeable onDepthEvent(Set<String> symbols, UniverseApiCallback<DepthResponse> callback) {
        String template = "market.%s.depth.step0";
        HuobiApiWebSocketListener<DepthResponse> cb = new HuobiApiWebSocketListener<>(callback, DepthResponse.class);
        return this.createNewWebSocket(template, symbols, cb);
    }

    @Override
    public void close() {
        this.client.dispatcher().executorService().shutdown();
    }
}
