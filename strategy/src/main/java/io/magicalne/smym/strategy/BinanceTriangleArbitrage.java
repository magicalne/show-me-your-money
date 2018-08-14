package io.magicalne.smym.strategy;

import com.binance.api.client.domain.event.CandlestickEvent;
import com.binance.api.client.domain.general.ExchangeInfo;
import com.binance.api.client.domain.general.SymbolInfo;
import com.binance.api.client.domain.market.OrderBook;
import com.binance.api.client.domain.market.OrderBookEntry;
import io.magicalne.smym.dto.Triangular;
import io.magicalne.smym.exchanges.BinanceEventHandler;
import io.magicalne.smym.exchanges.BinanceExchange;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class BinanceTriangleArbitrage {

    private static final double TRIPLE_COMMISSION = 0.999*0.999*0.999;
    private static final String USDT = "USDT";
    private static final String BTC = "BTC";
    private static final String ETH = "ETH";
    private final BinanceExchange exchange;
    private List<Triangular> btcusdtPairList;
    private List<Triangular> ethusdtPairList;
    private BinanceEventHandler<CandlestickEvent> candlestickHandler;

    public BinanceTriangleArbitrage(String accessId, String secretKey) {
        this.exchange = new BinanceExchange(accessId, secretKey);
    }

    public void setup() {
        ExchangeInfo exchangeInfo = this.exchange.getExchangeInfo();
        List<SymbolInfo> symbols = exchangeInfo.getSymbols();
        Map<String, List<SymbolInfo>> quoteGroup =
                symbols.stream().collect(Collectors.groupingBy(SymbolInfo::getQuoteAsset));

        List<SymbolInfo> usdtGrp = quoteGroup.get(USDT);
        List<SymbolInfo> btcGrp = quoteGroup.get(BTC);
        List<SymbolInfo> ethGrp = quoteGroup.get(ETH);

        //usdt with btc
        List<Triangular> btcusdtPairList = new LinkedList<>();
        String btcusdt = "BTCUSDT";
        List<Triangular> ethusdtPairList = new LinkedList<>();
        String ethusdt = "ETHUSDT";
        for (SymbolInfo u : usdtGrp) {
            for (SymbolInfo b : btcGrp) {
                if (u.getBaseAsset().equals(b.getBaseAsset())) {
                    Triangular triangular = new Triangular(btcusdt, b.getBaseAsset() + BTC, b.getBaseAsset() + USDT);
                    btcusdtPairList.add(triangular);
                }
            }

            for (SymbolInfo e : ethGrp) {
                if (u.getBaseAsset().equals(e.getBaseAsset())) {
                    Triangular triangular = new Triangular(ethusdt, e.getBaseAsset() + ETH, e.getBaseAsset() + USDT);
                    ethusdtPairList.add(triangular);
                }
            }
        }
        this.btcusdtPairList = btcusdtPairList;
        this.ethusdtPairList = ethusdtPairList;

        Set<String> symbolSet = new HashSet<>();
        btcusdtPairList.forEach(t -> {
            symbolSet.add(t.getSource());
            symbolSet.add(t.getMiddle());
            symbolSet.add(t.getLast());
        });
        ethusdtPairList.forEach(t -> {
            symbolSet.add(t.getSource());
            symbolSet.add(t.getMiddle());
            symbolSet.add(t.getLast());
        });

        this.candlestickHandler = new BinanceEventHandler<>(symbolSet.size());
        this.exchange.subscribeCandlestickEvent(symbolSet, this.candlestickHandler);
        this.exchange.createLocalOrderBook(symbolSet, 10);
    }

    public void test() {
        for (;;) {
            findArbitrage(this.btcusdtPairList);
            findArbitrage(this.ethusdtPairList);
        }
    }

    private void findArbitrage(List<Triangular> btcusdtPairList) {
        for (Triangular triangular : btcusdtPairList) {
            CandlestickEvent sourceEvent = this.candlestickHandler.getEventBySymbol(triangular.getSource());
            CandlestickEvent middleEvent = this.candlestickHandler.getEventBySymbol(triangular.getMiddle());
            CandlestickEvent lastEvent = this.candlestickHandler.getEventBySymbol(triangular.getLast());
            if (sourceEvent != null && middleEvent != null && lastEvent != null) {
                ArbitrageSpace arbitrageSpace = hasArbitrageSpace(sourceEvent, middleEvent, lastEvent);
                if (arbitrageSpace != ArbitrageSpace.NONE) {
                    double profit = takeArbitrage(arbitrageSpace, triangular);
                    log.info("Triangular: {}, profit: {}", triangular, profit);
                }

            }
        }
    }

    private double takeArbitrage(ArbitrageSpace arbitrageSpace, Triangular triangular) {
        log.info("Find arbitrage space: {}", arbitrageSpace);

        OrderBook sourceOrderBook = this.exchange.getOrderBook(triangular.getSource());
        OrderBook middleOrderBook = this.exchange.getOrderBook(triangular.getMiddle());
        OrderBook lastOrderBook = this.exchange.getOrderBook(triangular.getLast());

        if (arbitrageSpace == ArbitrageSpace.CLOCKWISE) {
            double source = getBestBidPrice(sourceOrderBook);
            double middle = getBestBidPrice(middleOrderBook);
            double last = getBestAskPrice(lastOrderBook);
            return getClockwise(source, middle, last);
        } else if (arbitrageSpace == ArbitrageSpace.REVERSE) {
            double last = getBestBidPrice(lastOrderBook);
            double middle = getBestAskPrice(middleOrderBook);
            double source = getBestAskPrice(sourceOrderBook);
            return getReverse(source, middle, last);
        }
        return -1;
    }

    private double getBestBidPrice(OrderBook orderBook) {
        if (orderBook != null) {
            List<OrderBookEntry> bids = orderBook.getBids();
            if (bids != null && !bids.isEmpty()) {
                return Double.parseDouble(bids.get(0).getPrice());
            }
        }
        return -1;
    }

    private double getBestAskPrice(OrderBook orderBook) {
        if (orderBook != null) {
            List<OrderBookEntry> asks = orderBook.getAsks();
            if (asks != null && !asks.isEmpty()) {
                return Double.parseDouble(asks.get(0).getPrice());
            }
        }
        return -1;
    }

    private ArbitrageSpace hasArbitrageSpace(CandlestickEvent sourceEvent,
                                             CandlestickEvent middleEvent,
                                             CandlestickEvent lastEvent) {
        final double upperBound = 1.01;
        double source = Double.parseDouble(sourceEvent.getClose());
        double middle = Double.parseDouble(middleEvent.getClose());
        double last = Double.parseDouble(lastEvent.getClose());
        double clockwise = getClockwise(source, middle, last);
        double reverse = getReverse(source, middle, last);
        if (clockwise > upperBound) {
            return ArbitrageSpace.CLOCKWISE;
        } else if (reverse > upperBound) {
            return ArbitrageSpace.REVERSE;
        } else {
            return ArbitrageSpace.NONE;
        }
    }

    private double getReverse(double source, double middle, double last) {
        return middle * source / last * TRIPLE_COMMISSION;
    }

    private double getClockwise(double source, double middle, double last) {
        return last / (source * middle) * TRIPLE_COMMISSION;
    }

    enum ArbitrageSpace {
        NONE,
        CLOCKWISE,
        REVERSE
    }

    public static void main(String[] args) {
        String accessKey = System.getenv("BINANCE_ACCESS_KEY");
        String secretKey = System.getenv("BINANCE_ACCESS_SECRET_KEY");
        BinanceTriangleArbitrage strategy = new BinanceTriangleArbitrage(accessKey, secretKey);
        strategy.setup();
        strategy.test();
    }
}
