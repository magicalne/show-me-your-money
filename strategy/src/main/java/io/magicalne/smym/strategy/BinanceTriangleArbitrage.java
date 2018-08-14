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
    private static final double UPPER_BOUND = 1.01;

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
                ArbitrageSpace arbitrageSpace = hasArbitrageSpace(
                        Double.parseDouble(sourceEvent.getClose()),
                        Double.parseDouble(middleEvent.getClose()),
                        Double.parseDouble(lastEvent.getClose()));
                if (arbitrageSpace != ArbitrageSpace.NONE) {
                    double profit = takeArbitrage(arbitrageSpace, triangular);
                    log.info("Use last close price. Triangular: {}, profit: {}", triangular, profit);
                    this.candlestickHandler.invalidateEventBySymble(triangular.getSource());
                    this.candlestickHandler.invalidateEventBySymble(triangular.getMiddle());
                    this.candlestickHandler.invalidateEventBySymble(triangular.getLast());
                }
            }
            OrderBook sourceOB = this.exchange.getOrderBook(triangular.getSource());
            OrderBook middleOB = this.exchange.getOrderBook(triangular.getMiddle());
            OrderBook lastOB = this.exchange.getOrderBook(triangular.getLast());
            double source = Double.parseDouble(sourceOB.getBids().get(1).getPrice());
            double middle = Double.parseDouble(middleOB.getBids().get(1).getPrice());
            double last = Double.parseDouble(lastOB.getAsks().get(1).getPrice());
            double profit = getClockwise(source, middle, last);
            if (profit > UPPER_BOUND) {
                log.info("Use {}st price in order book. Clockwise triangular: {}, profit: {}", 2, triangular, profit);
            }

            last = Double.parseDouble(lastOB.getBids().get(1).getPrice());
            middle = Double.parseDouble(middleOB.getAsks().get(1).getPrice());
            source = Double.parseDouble(sourceOB.getAsks().get(1).getPrice());
            profit = getReverse(source, middle, last);
            if (profit > UPPER_BOUND) {
                log.info("Use {}st price in order book. Reverse triangular: {}, profit: {}", 2, triangular, profit);
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
                double p0 = Double.parseDouble(bids.get(0).getPrice());
                double p1 = Double.parseDouble(bids.get(1).getPrice());
                return (p0 + p1) / 2;
            }
        }
        return -1;
    }

    private double getBestAskPrice(OrderBook orderBook) {
        if (orderBook != null) {
            List<OrderBookEntry> asks = orderBook.getAsks();
            if (asks != null && !asks.isEmpty()) {
                double p0 = Double.parseDouble(asks.get(0).getPrice());
                double p1 = Double.parseDouble(asks.get(1).getPrice());
                return (p0 + p1) / 2;
            }
        }
        return -1;
    }

    private ArbitrageSpace hasArbitrageSpace(double source, double middle, double last) {
        double clockwise = getClockwise(source, middle, last);
        double reverse = getReverse(source, middle, last);
        if (clockwise > UPPER_BOUND) {
            return ArbitrageSpace.CLOCKWISE;
        } else if (reverse > UPPER_BOUND) {
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
