package io.magicalne.smym.exchanges;

import com.binance.api.client.BinanceApiAsyncRestClient;
import com.binance.api.client.BinanceApiClientFactory;
import com.binance.api.client.BinanceApiRestClient;
import com.binance.api.client.BinanceApiWebSocketClient;
import com.binance.api.client.domain.account.Account;
import com.binance.api.client.domain.event.CandlestickEvent;
import com.binance.api.client.domain.event.DepthEvent;
import com.binance.api.client.domain.general.ExchangeInfo;
import com.binance.api.client.domain.market.CandlestickInterval;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

@Slf4j
public class BinanceExchange {

    private final BinanceApiWebSocketClient wsClient;
    private final BinanceApiRestClient restClient;
    private final BinanceApiAsyncRestClient asyncRestClient;
    private BinanceEventHandler<CandlestickEvent> candlestickHandler;
    private BinanceEventHandler<DepthEvent> depthEventHandler;
    public BinanceExchange(String accessKey, String secretKey) {
        BinanceApiClientFactory factory = BinanceApiClientFactory.newInstance(accessKey, secretKey);
        this.wsClient = factory.newWebSocketClient();
        this.restClient = factory.newRestClient();
        this.asyncRestClient = factory.newAsyncRestClient();
    }

    public void subscribeCandlestickEvent(Set<String> symbols, BinanceEventHandler<CandlestickEvent> handler) {
        this.candlestickHandler = handler;
        for (String s : symbols) {
            this.wsClient.onCandlestickEvent(s, CandlestickInterval.ONE_MINUTE,
                    candlestickEvent -> this.candlestickHandler.update(candlestickEvent.getSymbol(), candlestickEvent));
        }
        log.info("Subscribe {} candle stick event.", symbols.size());
    }

    public void subscribeDepthEvent(Set<String> symbols, BinanceEventHandler<DepthEvent> handler) {
        this.depthEventHandler = handler;
        for (String s : symbols) {
            this.wsClient.onDepthEvent(s, e -> this.depthEventHandler.update(s, e));
        }
        log.info("Subscribe {} market depth events.", symbols.size());
    }

    public Account getAccount() {
        return this.restClient.getAccount();
    }

    public ExchangeInfo getExchangeInfo() {
        return this.restClient.getExchangeInfo();
    }

    public static void main(String[] args) {
        String accessKey = System.getenv("BINANCE_ACCESS_KEY");
        String secretKey = System.getenv("BINANCE_ACCESS_SECRET_KEY");
        BinanceExchange exchange = new BinanceExchange(accessKey, secretKey);
        log.info("{}", exchange.getExchangeInfo());
    }
}
