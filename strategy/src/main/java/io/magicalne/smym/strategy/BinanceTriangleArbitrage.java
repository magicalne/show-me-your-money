package io.magicalne.smym.strategy;

import com.binance.api.client.domain.OrderStatus;
import com.binance.api.client.domain.TimeInForce;
import com.binance.api.client.domain.account.Account;
import com.binance.api.client.domain.account.AssetBalance;
import com.binance.api.client.domain.account.NewOrderResponse;
import com.binance.api.client.domain.general.ExchangeInfo;
import com.binance.api.client.domain.general.FilterType;
import com.binance.api.client.domain.general.SymbolFilter;
import com.binance.api.client.domain.general.SymbolInfo;
import com.binance.api.client.domain.market.OrderBook;
import com.binance.api.client.domain.market.OrderBookEntry;
import com.binance.api.client.exception.BinanceApiException;
import io.magicalne.smym.dto.TradeInfo;
import io.magicalne.smym.dto.Triangular;
import io.magicalne.smym.exception.BuyFailureException;
import io.magicalne.smym.exception.SellFailureException;
import io.magicalne.smym.exchanges.BinanceExchange;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Slf4j
public class BinanceTriangleArbitrage {

    private static final double TRIPLE_COMMISSION = 0.999*0.999*0.999;
    private static final BigDecimal COMMISSION = new BigDecimal("0.999").setScale(3, RoundingMode.HALF_EVEN);
    private static final String USDT = "USDT";
    private static final String BTC = "BTC";
    private static final String ETH = "ETH";
    private static final double UPPER_BOUND = 1.01;
    private static final double BUY_SLIPPAGE = 1.000;
    private static final double SELL_SLIPPAGE = 1;

    private String usdtCapital = "15";
    private String btcCapital = "0.002";
    private String ethCapital = "0.05";

    private final BinanceExchange exchange;
    private List<Triangular> btcusdtPairList;
    private List<Triangular> ethusdtPairList;
    private ExchangeInfo exchangeInfo;

    public BinanceTriangleArbitrage(String accessId, String secretKey) {
        this.exchange = new BinanceExchange(accessId, secretKey);
    }

    public void setup() {
        this.exchangeInfo = this.exchange.getExchangeInfo();
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
        this.exchange.createLocalOrderBook(symbolSet, 10);
    }

    private void initCapital() {
        List<String> balances = getCapitalFromBalance(Arrays.asList(USDT, BTC, ETH));
        String newUSDT = balances.get(0);
        String newBTC = balances.get(1);
        String newETH = balances.get(2);
        log.info("Before trade, usdt: {}, btc: {}, eth: {}", this.usdtCapital, this.btcCapital, this.ethCapital);
        this.usdtCapital = getMin(newUSDT, this.usdtCapital);
        this.btcCapital = getMin(newBTC, this.btcCapital);
        this.ethCapital = getMin(newETH, this.ethCapital);

        log.info("After trade,  usdt: {}, btc: {}, eth: {}", this.usdtCapital, this.btcCapital, this.ethCapital);
    }

    private String getMin(String balanceFromAccount, String preset) {
        return Double.parseDouble(balanceFromAccount) > Double.parseDouble(preset) ? preset : balanceFromAccount;
    }

    private List<String> getCapitalFromBalance(List<String> assets) {
        Account account = this.exchange.getAccount();
        List<String> assetBalances = new ArrayList<>();
        for (String a : assets) {
            AssetBalance assetBalance = account.getAssetBalance(a);
            assetBalances.add(assetBalance.getFree());
        }
        return assetBalances;
    }

    public void run() {

        initCapital();
        for (; ; ) {
            findArbitrage(this.btcusdtPairList, BTC);
            findArbitrage(this.ethusdtPairList, ETH);
        }
    }

    private void findArbitrage(List<Triangular> pairList, String assetType) {
        String assetQty;
        if (BTC.equals(assetType)) {
            assetQty = this.btcCapital;
        } else if (ETH.equals(assetType)) {
            assetQty = this.ethCapital;
        } else {
            throw new IllegalArgumentException("Wrong argument: baseType: " + assetType);
        }
        for (Triangular triangular : pairList) {
            //use order book price level
            final int priceLevel = 0;
            OrderBook sourceOB = this.exchange.getOrderBook(triangular.getSource());
            OrderBook middleOB = this.exchange.getOrderBook(triangular.getMiddle());
            OrderBook lastOB = this.exchange.getOrderBook(triangular.getLast());

            List<OrderBookEntry> sourceOBAsks = sourceOB.getAsks();
            List<OrderBookEntry> middleOBAsks = middleOB.getAsks();
            List<OrderBookEntry> lastOBBids = lastOB.getBids();
            if (sourceOBAsks != null && sourceOBAsks.size() > priceLevel+1 &&
                    middleOBAsks != null && middleOBAsks.size() > priceLevel+1 &&
                    lastOBBids != null && lastOBBids.size() > priceLevel+1) {
                double source = Double.parseDouble(sourceOBAsks.get(priceLevel).getPrice());
                double middle = Double.parseDouble(middleOBAsks.get(priceLevel).getPrice());
                double last = Double.parseDouble(lastOBBids.get(priceLevel+1).getPrice());
                double profit = getClockwise(source, middle, last);
                if (profit > UPPER_BOUND) {
                    log.info("Use {}st price in order book. Clockwise, {}: {} -> {}: {} -> {}: {}, profit: {}",
                            priceLevel+1,
                            triangular.getSource(), source,
                            triangular.getMiddle(), middle,
                            triangular.getLast(), last,
                            profit);
                    takeIt(triangular, source, middle, last, this.usdtCapital, assetQty, assetType,true);
                }
            }

            List<OrderBookEntry> sourceOBBids = sourceOB.getBids();
            List<OrderBookEntry> middleOBBids = middleOB.getBids();
            List<OrderBookEntry> lastOBAsks = lastOB.getAsks();
            if (sourceOBBids != null && sourceOBBids.size() > priceLevel+1 &&
                    middleOBBids != null && middleOBBids.size() > priceLevel+1 &&
                    lastOBAsks != null && lastOBAsks.size() > priceLevel+1) {
                double source = Double.parseDouble(sourceOBBids.get(priceLevel).getPrice());
                double middle = Double.parseDouble(middleOBBids.get(priceLevel+1).getPrice());
                double last = Double.parseDouble(lastOBAsks.get(priceLevel).getPrice());
                double profit = getReverse(source, middle, last);
                if (profit > UPPER_BOUND) {
                    log.info("Use {}st price in order book. Reverse, {}: {} -> {}: {} -> {}: {}, profit: {}",
                            priceLevel+1, triangular.getLast(), last, triangular.getMiddle(), middle,
                            triangular.getSource(), source, profit);
                    takeIt(triangular, source, middle, last, this.usdtCapital, assetQty, assetType,false);
                }
            }
        }
    }

    public void takeIt(Triangular triangular, double sourcePrice, double middlePrice, double lastPrice,
                       String usdt, String assetQty, String assetType, boolean clockwise) {

        boolean traded;
        if (clockwise) {
            traded = clockwiseArbitrage(triangular, sourcePrice, middlePrice, lastPrice, usdt, assetQty, assetType);
        } else {
            traded = reverseArbitrage(triangular, sourcePrice, middlePrice, lastPrice, usdt, assetQty, assetType);
        }
        if (traded) {
            initCapital();
        }
    }

    private boolean clockwiseArbitrage(Triangular triangular, double sourcePrice, double middlePrice, double lastPrice,
                                       String usdt, String base, String baseType) {
        CompletableFuture<TradeInfo> buyBase = CompletableFuture
                .supplyAsync(() -> quickBuy(triangular.getSource(), sourcePrice, usdt, false));

        CompletableFuture<TradeInfo> getSpreed = CompletableFuture
                .supplyAsync(() -> {
                    TradeInfo middleTradeInfo = quickBuy(triangular.getMiddle(), middlePrice, base, false);
                    return quickSell(triangular.getLast(), lastPrice, middleTradeInfo.getQty(), true);
                });
        for (;;) {
            if (buyBase.isDone() && getSpreed.isDone()) {
                if (buyBase.isCompletedExceptionally() && getSpreed.isCompletedExceptionally()) {
                    log.info("Both failed, so give up.");
                    return false;
                } else if (buyBase.isCompletedExceptionally()) {
                    log.info("Buy btcusdt failed, buy it now.");
                    quickBuy(triangular.getSource(), sourcePrice, usdt, true);
                } else if (getSpreed.isCompletedExceptionally()) {
                    Triangular pair = findBestPairToUsdt(baseType);
                    log.info("Buy alt coin failed, try again with new pair: {}.", pair);
                    String source = pair.getSource();
                    if (source != null) {
                        String p = this.exchange.getBestBid(source).getPrice();
                        quickSell(source, Double.parseDouble(p), base, true);
                    } else {
                        String pm = pair.getMiddle();
                        String pmPrice = this.exchange.getBestAsk(pm).getPrice();
                        String pl = pair.getLast();
                        String plPrice = this.exchange.getBestBid(pl).getPrice();
                        TradeInfo middleTradeInfo = quickBuy(pm, Double.parseDouble(pmPrice), base, true);
                        quickSell(pl, Double.parseDouble(plPrice), middleTradeInfo.getQty(), true);
                    }
                }
                return true;
            }
        }
    }

    private boolean reverseArbitrage(Triangular triangular, double sourcePrice, double middlePrice, double lastPrice,
                                     String usdt, String base, String baseType) {
        CompletableFuture<TradeInfo> buyBase = CompletableFuture
                .supplyAsync(() -> {
                    TradeInfo tradeInfo = quickBuy(triangular.getLast(), lastPrice, usdt, false);
                    return quickSell(triangular.getMiddle(), middlePrice, tradeInfo.getQty(), true);
                });

        CompletableFuture<TradeInfo> getSpreed = CompletableFuture
                .supplyAsync(() -> quickSell(triangular.getSource(), sourcePrice, new BigDecimal(base), false));
        for (; ; ) {
            if (buyBase.isDone() && getSpreed.isDone()) {
                if (buyBase.isCompletedExceptionally() && getSpreed.isCompletedExceptionally()) {
                    log.info("Both failed, so give up.");
                    return false;
                } else if (buyBase.isCompletedExceptionally()) {
                    log.info("Buy base failed, buy it now.");

                    Triangular pair = findBestPairToBase(baseType);
                    String source = pair.getSource();
                    if (source != null) {
                        String p = this.exchange.getBestAsk(source).getPrice();
                        quickBuy(source, Double.parseDouble(p), usdt, true);
                    } else {
                        String pl = pair.getLast();
                        Double plPrice = Double.valueOf(this.exchange.getBestAsk(pl).getPrice());
                        String pm = pair.getMiddle();
                        Double pmPrice = Double.valueOf(this.exchange.getBestBid(pm).getPrice());
                        TradeInfo tradeInfo = quickBuy(pl, plPrice, usdt, true);
                        quickSell(pm, pmPrice, tradeInfo.getQty(), true);
                    }
                } else if (getSpreed.isCompletedExceptionally()) {
                    quickSell(triangular.getSource(), sourcePrice, new BigDecimal(base), true);
                }
                return true;
            }
        }
    }

    private int getQtyPrecision(String symbol) {
        SymbolInfo symbolInfo = this.exchangeInfo.getSymbolInfo(symbol);
        SymbolFilter lotSize = symbolInfo.getSymbolFilter(FilterType.LOT_SIZE);
        String minQty = lotSize.getMinQty();
        int index = minQty.indexOf('1');
        return index == 0 ? index : index - 1;
    }

    private int getPricePrecision(String symbol) {
        SymbolInfo symbolInfo = this.exchangeInfo.getSymbolInfo(symbol);
        SymbolFilter priceFilter = symbolInfo.getSymbolFilter(FilterType.PRICE_FILTER);
        String tickSize = priceFilter.getTickSize();
        int index = tickSize.indexOf("1");
        return index == 0 ? index : index - 1;
    }

    private TradeInfo quickBuy(String symbol, double price, String quoteQty, boolean force) {
        int basePrecision = getQtyPrecision(symbol);
        int quotePrecision = getPricePrecision(symbol);
        BigDecimal p = new BigDecimal(price).setScale(quotePrecision, RoundingMode.HALF_EVEN);
        BigDecimal q = new BigDecimal(quoteQty).setScale(quotePrecision, RoundingMode.DOWN);
        BigDecimal qty = q.divide(p, RoundingMode.DOWN).setScale(basePrecision, RoundingMode.DOWN);

        String qtyStr = qty.toPlainString();
        String priceStr = p.toPlainString();
        NewOrderResponse res = null;
        try {
            res = this.exchange.limitBuy(symbol, TimeInForce.IOC, qtyStr, priceStr, 5000);
            OrderStatus status = res.getStatus();
            if (status == OrderStatus.FILLED || status == OrderStatus.PARTIALLY_FILLED) {
                return getTradeInfoFromOrder(res, basePrecision, quotePrecision);
            }
            if (force) {
                res = this.exchange.marketBuy(symbol, qtyStr);
                return getTradeInfoFromOrder(res, basePrecision, quotePrecision);
            }
        } catch (BinanceApiException e) {
            log.error("Failed to buy {} @{} of {}, due to:", symbol, priceStr, qtyStr, e);
        }
        throw new BuyFailureException(symbol, res == null ? null : res.getOrderId());
    }

    private TradeInfo quickSell(String symbol, double price, String baseQty, boolean force) {
        int basePrecision = getQtyPrecision(symbol);
        int quotePrecision = getPricePrecision(symbol);
        BigDecimal p = new BigDecimal(price).setScale(quotePrecision, RoundingMode.HALF_EVEN);
        String baseQtyStr = new BigDecimal(baseQty).setScale(basePrecision, RoundingMode.DOWN).toPlainString();
        String priceStr = p.toPlainString();
        NewOrderResponse res = null;
        try {
            res = this.exchange.limitSell(symbol, TimeInForce.IOC, baseQtyStr, priceStr, 5000);
            OrderStatus status = res.getStatus();
            if (status == OrderStatus.FILLED || status == OrderStatus.PARTIALLY_FILLED) {
                return getTradeInfoFromOrder(res, basePrecision, quotePrecision);
            }
            if (force) {
                res = this.exchange.marketSell(symbol, baseQtyStr);
                return getTradeInfoFromOrder(res, basePrecision, quotePrecision);
            }
        } catch (BinanceApiException e) {
            log.error("Failed to sell {} @{} of {}, due to:", symbol, priceStr, baseQtyStr, e);
        }
        throw new SellFailureException(symbol, res == null ? null : res.getOrderId());
    }

    private TradeInfo quickSell(String symbol, double price, BigDecimal baseQty, boolean force) {
        int basePrecision = getQtyPrecision(symbol);
        int quotePrecision = getPricePrecision(symbol);
        BigDecimal p = new BigDecimal(price).setScale(quotePrecision, RoundingMode.HALF_EVEN);
        String baseQtyStr = baseQty.setScale(basePrecision, RoundingMode.DOWN).toPlainString();
        String priceStr = p.toPlainString();
        NewOrderResponse res = null;
        try {
            res = this.exchange.limitSell(symbol, TimeInForce.IOC, baseQtyStr, priceStr, 5000);
            OrderStatus status = res.getStatus();
            if (status == OrderStatus.FILLED || status == OrderStatus.PARTIALLY_FILLED) {
                return getTradeInfoFromOrder(res, basePrecision, quotePrecision);
            }
            if (force) {
                res = this.exchange.marketSell(symbol, baseQtyStr);
                return getTradeInfoFromOrder(res, basePrecision, quotePrecision);
            }
        } catch (BinanceApiException e) {
            log.error("Failed to sell {} @{} of {}, due to:", symbol, priceStr, baseQtyStr, e);
        }
        throw new SellFailureException(symbol, res == null ? null : res.getOrderId());
    }

    private TradeInfo getTradeInfoFromOrder(NewOrderResponse res, int basePrecision, int quotePrecision) {
        BigDecimal quotePrice = new BigDecimal(res.getPrice())
                .setScale(quotePrecision, RoundingMode.UP);
        TradeInfo tradeInfo = new TradeInfo();
        tradeInfo.setPrice(quotePrice);
        BigDecimal totalQty;
        BigDecimal baseQty = new BigDecimal(res.getExecutedQty()).setScale(basePrecision, RoundingMode.DOWN);
        totalQty = baseQty.multiply(COMMISSION);
        tradeInfo.setQty(totalQty);

        return tradeInfo;
    }

    private Triangular findBestPairToUsdt(String base) {
        List<Triangular> tList;
        Triangular bestPair = null;
        double max = -1;
        if (BTC.equals(base)) {
            tList = btcusdtPairList;
        } else if (ETH.equals(base)) {
            tList = ethusdtPairList;
        } else {
            throw new IllegalArgumentException("Unknown base:" + base);
        }
        for (Triangular p : tList) {
            String middle = p.getMiddle();
            String last = p.getLast();
            OrderBookEntry bestAsk = this.exchange.getBestAsk(middle);
            OrderBookEntry bestBid = this.exchange.getBestBid(last);
            if (bestAsk == null || bestBid == null) {
                continue;
            }
            Double middlePrice = Double.valueOf(bestAsk.getPrice());
            Double lastPrice = Double.valueOf(bestBid.getPrice());
            double btcusdtPrice = lastPrice / middlePrice;
            if (btcusdtPrice > max) {
                max = btcusdtPrice;
                bestPair = p;
            }
        }
        OrderBookEntry btcusdt = this.exchange.getBestBid("BTCUSDT");
        if (btcusdt != null) {
            Double price = Double.valueOf(btcusdt.getPrice());
            if (price < max*COMMISSION.doubleValue()) {
                return new Triangular("BTCUSDT", null, null);
            }
        }
        return new Triangular(null, bestPair.getMiddle(), bestPair.getLast());
    }

    private Triangular findBestPairToBase(String base) {
        List<Triangular> tList;
        Triangular bestPair = null;
        double min = Double.MAX_VALUE;
        if (BTC.equals(base)) {
            tList = btcusdtPairList;
        } else if (ETH.equals(base)) {
            tList = ethusdtPairList;
        } else {
            throw new IllegalArgumentException("Unknown base:" + base);
        }

        for (Triangular p : tList) {
            String last = p.getLast();
            String middle = p.getMiddle();
            OrderBookEntry bestBid = this.exchange.getBestBid(last);
            OrderBookEntry bestAsk = this.exchange.getBestAsk(middle);
            if (bestAsk == null || bestBid == null) {
                continue;
            }
            Double lastPrice = Double.valueOf(bestBid.getPrice());
            Double middlePrice = Double.valueOf(bestAsk.getPrice());
            double rate = lastPrice / middlePrice;
            if (rate < min) {
                min = rate;
                bestPair = p;
            }
        }
        String symbol = base + "usdt";
        OrderBookEntry baseAsk = this.exchange.getBestAsk(symbol);
        if (baseAsk != null) {
            Double price = Double.valueOf(baseAsk.getPrice());
            if (price > min*COMMISSION.doubleValue()) {
                return new Triangular(symbol, null, null);
            }
        }
        return new Triangular(null, bestPair.getMiddle(), bestPair.getLast());
    }

    private double getReverse(double source, double middle, double last) {
        return middle * source / last * TRIPLE_COMMISSION;
    }

    private double getClockwise(double source, double middle, double last) {
        return last / (source * middle) * TRIPLE_COMMISSION;
    }

    public static void main(String[] args) {
        String accessKey = System.getenv("BINANCE_ACCESS_KEY");
        String secretKey = System.getenv("BINANCE_ACCESS_SECRET_KEY");
        BinanceTriangleArbitrage strategy = new BinanceTriangleArbitrage(accessKey, secretKey);
        strategy.setup();
        strategy.run();
    }
}
