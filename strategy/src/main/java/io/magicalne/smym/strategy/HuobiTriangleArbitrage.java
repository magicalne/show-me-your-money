package io.magicalne.smym.strategy;

import io.magicalne.smym.dto.*;
import io.magicalne.smym.exception.BuyFailureException;
import io.magicalne.smym.exception.OrderPlaceException;
import io.magicalne.smym.exception.SellFailureException;
import io.magicalne.smym.exchanges.HuobiExchange;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class HuobiTriangleArbitrage {
    private static final double COMMISSION = 0.998;
    private static final double TRIPLE_COMMISSION = COMMISSION*COMMISSION*COMMISSION;
    private static final String PARTIAL_CANCELED = "partial-canceled";
    private static final String CANCELED = "canceled";
    private static final String PARTIAL_FILLED = "partial-filled";
    private static final String FILLED = "filled";
    private static final double UPPER_BOUND = 1.005;
    private static final double BUY_SLIPPAGE = 1;
    private static final double SELL_SLIPPAGE = 1;

    private final HuobiExchange exchange;
    private List<Triangular> btcusdtPairList;
    private List<Triangular> ethusdtPairList;
    private List<Triangular> htusdtPairList;
    private final Map<String, Symbol> symbolMap = new HashMap<>();

    private final List<String> cannotTradeBaseCurrency = Collections.singletonList("vet");
    private final String accountId;
    private String usdt;
    private String btc;
    private String eth;


    public HuobiTriangleArbitrage(String accountId, String accessKey, String secretKey) {
        this.accountId = accountId;
        this.exchange = new HuobiExchange(accountId, accessKey, secretKey);
    }

    public void init() {
        List<Symbol> symbols = this.exchange.getSymbolInfo();
        List<Integer> tobeDeletedIndexList = new ArrayList<>(cannotTradeBaseCurrency.size());
        for (int i = 0; i < symbols.size(); i ++) {
            Symbol s = symbols.get(i);
            String baseCurrency = s.getBaseCurrency();
            String symbol = s.getSymbol();
            if (cannotTradeBaseCurrency.contains(baseCurrency)) {
                tobeDeletedIndexList.add(i);
            } else {
                symbolMap.put(symbol, s);
            }
        }
        for (int i = tobeDeletedIndexList.size()-1; i >= 0; i --) {
            int index = tobeDeletedIndexList.get(i);
            symbols.remove(index);
        }

        Map<String, List<Symbol>> quoteGroup =
                symbols.stream().collect(Collectors.groupingBy(Symbol::getQuoteCurrency));

        List<Symbol> usdtGrp = quoteGroup.get("usdt");
        List<Symbol> btcGrp = quoteGroup.get("btc");
        List<Symbol> ethGrp = quoteGroup.get("eth");
//        List<Symbol> htGrp = quoteGroup.get("ht");

        //usdt with btc
        List<Triangular> btcusdtPairList = new LinkedList<>();
        String btcusdt = "btcusdt";
        List<Triangular> ethusdtPairList = new LinkedList<>();
        String ethusdt = "ethusdt";
        List<Triangular> htusdtPairList = new LinkedList<>();
        String htusdt = "htusdt";
        for (Symbol u : usdtGrp) {
            for (Symbol b : btcGrp) {
                if (u.getBaseCurrency().equals(b.getBaseCurrency())) {
                    Triangular triangular = new Triangular(btcusdt, b.getBaseCurrency() + "btc", b.getBaseCurrency() + "usdt");
                    btcusdtPairList.add(triangular);
                }
            }

            for (Symbol e : ethGrp) {
                if (u.getBaseCurrency().equals(e.getBaseCurrency())) {
                    Triangular triangular = new Triangular(ethusdt, e.getBaseCurrency() + "eth", e.getBaseCurrency() + "usdt");
                    ethusdtPairList.add(triangular);
                }
            }
//
//            for (Symbol h : htGrp) {
//                if (u.getBaseCurrency().equals(h.getBaseCurrency())) {
//                    Triangular triangular = new Triangular(htusdt, h.getBaseCurrency() + "ht", h.getBaseCurrency() + "usdt");
//                    htusdtPairList.add(triangular);
//                }
//            }
        }

        Set<String> symbolSet = new HashSet<>();
        btcusdtPairList.forEach(t -> {
            symbolSet.add(t.getSource());
            symbolSet.add(t.getMiddle());
            symbolSet.add(t.getLast());
        });
        this.btcusdtPairList = btcusdtPairList;
        ethusdtPairList.forEach(t -> {
            symbolSet.add(t.getSource());
            symbolSet.add(t.getMiddle());
            symbolSet.add(t.getLast());
        });
        this.ethusdtPairList = ethusdtPairList;
//        htusdtPairList.forEach(t -> {
//            symbolSet.add(t.getSource());
//            symbolSet.add(t.getMiddle());
//            symbolSet.add(t.getLast());
//        });
//        this.htusdtPairList = htusdtPairList;

        exchange.createOrderBook(symbolSet, 5);
    }

    private void initCapital() {
        String newUSDT = getCapitalFromBalance("usdt");
        String newBTC = getCapitalFromBalance("btc");
        String newETH = getCapitalFromBalance("eth");
        if (this.usdt == null || this.btc == null || this.eth == null) {
            log.info("Capital usdt: {}, btc: {}, eth: {}", newUSDT, newBTC, newETH);
        } else {
            log.info("Before trade, usdt: {}, btc: {}, eth: {} | now, usdt: {}, btc: {}, eth: {}",
                    this.usdt, this.btc, this.eth, newUSDT, newBTC, newETH);
        }
        this.usdt = newUSDT;
        this.btc = newBTC;
        this.eth = newETH;
    }

    private String getCapitalFromBalance(String currency) {
        BalanceResponse balanceRes = this.exchange.getAccount(accountId);
        List<BalanceBean> balances = balanceRes.getData().getList();
        for (BalanceBean b : balances) {
            if (currency.equals(b.getCurrency())) {
                return b.getBalance();
            }
        }
        return null;
    }

    private void run() {
        initCapital();
        for (;;) {
            findArbitrage(this.btcusdtPairList, "btc");
            findArbitrage(this.ethusdtPairList, "eth");
        }
    }

    private void findArbitrage(List<Triangular> pairList, String assetType) {
        String assetQty;
        if ("btc".equals(assetType)) {
            assetQty = this.btc;
        } else if ("eth".equals(assetType)) {
            assetQty = this.eth;
        } else {
            throw new IllegalArgumentException("Wrong argument: baseType: " + assetType);
        }
        for (Triangular triangular : pairList) {
            final int priceLevel = 0;
            Depth sourceDepth = this.exchange.getOrderBook(triangular.getSource());
            Depth middleDepth = this.exchange.getOrderBook(triangular.getMiddle());
            Depth lastDepth = this.exchange.getOrderBook(triangular.getLast());

            if (sourceDepth == null || middleDepth == null || lastDepth == null) {
                continue;
            }
            //clockwise
            List<List<Double>> sourceDepthAsks = sourceDepth.getAsks();
            List<List<Double>> middleDepthAsks = middleDepth.getAsks();
            List<List<Double>> lastDepthBids = lastDepth.getBids();
            if (sourceDepthAsks.size() < priceLevel+1 ||
                    middleDepthAsks.size() < priceLevel+1 ||
                    lastDepthBids.size() < priceLevel+1) {
                continue;
            }
            double source = sourceDepthAsks.get(priceLevel).get(0) * BUY_SLIPPAGE;
            double middle = middleDepthAsks.get(priceLevel).get(0) * BUY_SLIPPAGE;
            double last = lastDepthBids.get(priceLevel).get(0) * SELL_SLIPPAGE;
            double profit = getClockwise(source, middle, last);
            if (profit > UPPER_BOUND) {
                log.info("Use {}st price in order book. Clockwise, {}: {} -> {}: {} -> {}: {}, profit: {}",
                        priceLevel+1,
                        triangular.getSource(), source,
                        triangular.getMiddle(), middle,
                        triangular.getLast(), last,
                        profit);
                takeIt(triangular, source, middle, last, this.usdt, assetQty, assetType,true);
            }
            //reverse clockwise
            List<List<Double>> sourceDepthBids = sourceDepth.getBids();
            List<List<Double>> middleDepthBids = middleDepth.getBids();
            List<List<Double>> lastDepthAsks = lastDepth.getAsks();
            source = sourceDepthBids.get(priceLevel).get(0) * SELL_SLIPPAGE;
            middle = middleDepthBids.get(priceLevel).get(0) * SELL_SLIPPAGE;
            last = lastDepthAsks.get(priceLevel).get(0) * BUY_SLIPPAGE;
            profit = getReverse(source, middle, last);
            if (profit > UPPER_BOUND) {
                log.info("Use {}st price in order book. Reverse Clockwise, {}: {} -> {}: {} -> {}: {}, profit: {}",
                        priceLevel+1,
                        triangular.getLast(), last,
                        triangular.getMiddle(), middle,
                        triangular.getSource(), source,
                        profit);
                takeIt(triangular, source, middle, last, this.usdt, assetQty, assetType,false);
            }
        }
    }

    public void takeIt(Triangular triangular, double sourcePrice, double middlePrice, double lastPrice,
                        String usdt, String assetQty, String assetType, boolean clockwise) {

        int tenMin = 600000;
        if (clockwise) {
            clockwiseArbitrage(triangular, sourcePrice, middlePrice, lastPrice, usdt, assetQty, assetType, tenMin);
        } else {
            reverseArbitrage(triangular, sourcePrice, middlePrice, lastPrice, usdt, assetQty, assetType, tenMin);
        }
    }

    private void reverseArbitrage(Triangular triangular, double sourcePrice, double middlePrice, double lastPrice,
                                  String usdt, String base, String baseType, int tenMin) {
        CompletableFuture<TradeInfo> buyBase = CompletableFuture
                .supplyAsync(() -> {
                    TradeInfo tradeInfo = firstRoundBuy(triangular.getLast(), lastPrice, usdt, false);
                    return sell(triangular.getMiddle(), middlePrice, tradeInfo.getQty(), tenMin);
                });

        CompletableFuture<TradeInfo> getSpreed = CompletableFuture
                .supplyAsync(() -> sell(triangular.getSource(), sourcePrice, new BigDecimal(base), tenMin));
        for (; ; ) {
            if (buyBase.isDone() && getSpreed.isDone()) {
                if (buyBase.isCompletedExceptionally() && getSpreed.isCompletedExceptionally()) {
                    log.info("Both failed, so give up.");
                    return;
                } else if (buyBase.isCompletedExceptionally()) {
                    log.info("Buy base failed, buy it now.");

                    Triangular pair = findBestPairToBase(baseType);
                    String source = pair.getSource();
                    if (source != null) {
                        Double p = this.exchange.getBestAsk(source).get(0);
                        firstRoundBuy(source, p, usdt, true);
                    } else {
                        String pl = pair.getLast();
                        Double plPrice = this.exchange.getBestAsk(pl).get(0);
                        String pm = pair.getMiddle();
                        Double pmPrice = this.exchange.getBestBid(pm).get(0);
                        TradeInfo tradeInfo = firstRoundBuy(pl, plPrice, usdt, true);
                        sell(pm, pmPrice, tradeInfo.getQty(), tenMin);
                    }
                } else if (getSpreed.isCompletedExceptionally()) {
                    sell(triangular.getSource(), sourcePrice, new BigDecimal(base), tenMin);
                }
                initCapital();
                return;
            }
        }
    }

    private void clockwiseArbitrage(Triangular triangular, double sourcePrice, double middlePrice, double lastPrice,
                                    String usdt, String base, String baseType, int timeout) {
        CompletableFuture<TradeInfo> buyBase = CompletableFuture
                .supplyAsync(() -> firstRoundBuy(triangular.getSource(), sourcePrice, usdt, false));

        CompletableFuture<TradeInfo> getSpreed = CompletableFuture
                .supplyAsync(() -> {
                    TradeInfo middleTradeInfo = firstRoundBuy(triangular.getMiddle(), middlePrice, base, false);
                    return sell(triangular.getLast(), lastPrice, middleTradeInfo.getQty(), timeout);
                });
        for (;;) {
            if (buyBase.isDone() && getSpreed.isDone()) {
                if (buyBase.isCompletedExceptionally() && getSpreed.isCompletedExceptionally()) {
                    log.info("Both failed, so give up.");
                } else if (buyBase.isCompletedExceptionally()) {
                    log.info("Buy btcusdt failed, buy it now.");
                    firstRoundBuy(triangular.getSource(), sourcePrice, usdt, true);
                } else if (getSpreed.isCompletedExceptionally()) {
                    Triangular pair = findBestPairToUsdt(baseType);
                    log.info("Buy alt coin failed, try again with new pair: {}.", pair);
                    String source = pair.getSource();
                    if (source != null) {
                        Double p = this.exchange.getBestBid(source).get(0);
                        sell(source, p, new BigDecimal(base), timeout);
                    } else {
                        String pm = pair.getMiddle();
                        Double pmPrice = this.exchange.getBestAsk(pm).get(0);
                        String pl = pair.getLast();
                        Double plPrice = this.exchange.getBestBid(pl).get(0);
                        TradeInfo middleTradeInfo = firstRoundBuy(pm, pmPrice, base, true);
                        sell(pl, plPrice, middleTradeInfo.getQty(), timeout);
                    }
                }
                initCapital();
                return;
            }
        }
    }

    private Triangular findBestPairToUsdt(String base) {
        List<Triangular> tList;
        Triangular bestPair = null;
        double max = -1;
        if ("btc".equals(base)) {
            tList = btcusdtPairList;
        } else if ("eth".equals(base)) {
            tList = ethusdtPairList;
        } else {
            throw new IllegalArgumentException("Unknown base:" + base);
        }
        for (Triangular p : tList) {
            String middle = p.getMiddle();
            String last = p.getLast();
            List<Double> bestAsk = this.exchange.getBestAsk(middle);
            List<Double> bestBid = this.exchange.getBestBid(last);
            if (bestAsk == null || bestBid == null) {
                continue;
            }
            Double middlePrice = bestAsk.get(0);
            Double lastPrice = bestBid.get(0);
            double btcusdtPrice = lastPrice / middlePrice;
            if (btcusdtPrice > max) {
                max = btcusdtPrice;
                bestPair = p;
            }
        }
        List<Double> btcusdt = this.exchange.getBestBid("btcusdt");
        if (btcusdt != null) {
            Double price = btcusdt.get(0);
            if (price < max*COMMISSION) {
                return new Triangular("btcusdt", null, null);
            }
        }
        return new Triangular(null, bestPair.getMiddle(), bestPair.getLast());
    }

    private Triangular findBestPairToBase(String base) {
        List<Triangular> tList;
        Triangular bestPair = null;
        double min = Double.MAX_VALUE;
        if ("btc".equals(base)) {
            tList = btcusdtPairList;
        } else if ("eth".equals(base)) {
            tList = ethusdtPairList;
        } else {
            throw new IllegalArgumentException("Unknown base:" + base);
        }

        for (Triangular p : tList) {
            String last = p.getLast();
            String middle = p.getMiddle();
            List<Double> bestBid = this.exchange.getBestBid(last);
            List<Double> bestAsk = this.exchange.getBestAsk(middle);
            if (bestAsk == null || bestBid == null) {
                continue;
            }
            Double lastPrice = bestBid.get(0);
            Double middlePrice = bestAsk.get(0);
            double rate = lastPrice / middlePrice;
            if (rate < min) {
                min = rate;
                bestPair = p;
            }
        }
        String symbol = base + "usdt";
        List<Double> baseAsk = this.exchange.getBestAsk(symbol);
        if (baseAsk != null) {
            Double price = baseAsk.get(0);
            if (price > min*COMMISSION) {
                return new Triangular(symbol, null, null);
            }
        }
        return new Triangular(null, bestPair.getMiddle(), bestPair.getLast());
    }

    private TradeInfo firstRoundBuy(String symbol, double price, String quoteQty, boolean force) {
        Symbol symbolInfo = this.symbolMap.get(symbol);
        int basePrecision = symbolInfo.getAmountPrecision();
        int quotePrecision = symbolInfo.getPricePrecision();
        BigDecimal p = new BigDecimal(price).setScale(quotePrecision, RoundingMode.HALF_EVEN);
        int biggerPrecision = basePrecision > quotePrecision ? basePrecision : quotePrecision;
        BigDecimal q = new BigDecimal(quoteQty).setScale(biggerPrecision, RoundingMode.DOWN);
        BigDecimal qty = q.divide(p, RoundingMode.DOWN).setScale(basePrecision, RoundingMode.DOWN);

        String qtyStr = qty.toPlainString();
        String priceStr = p.toPlainString();
        OrderPlaceResponse res = this.exchange.limitBuy(symbol, qtyStr, priceStr);
        if (res.checkStatusOK()) {
            String orderId = res.getData();
            long record = System.currentTimeMillis();
            OrderDetail detail;
            for (;;) {
                detail = this.exchange.queryOrder(orderId);
                String state = detail.getState();
                if (FILLED.equals(state) || PARTIAL_FILLED.equals(state)) {
                    break;
                }
                if (System.currentTimeMillis() - record > 5000) {
                    detail = this.exchange.cancel(orderId);
                    break;
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(150);
                } catch (InterruptedException e) {
                    log.error("Interrupted exception.", e);
                }
            }
            if (FILLED.equals(detail.getState()) || PARTIAL_CANCELED.equals(detail.getState())) {
                return getTradeInfoFromOrder(detail, basePrecision, quotePrecision, true);
            }
            if (force) {
                OrderDetail marketBuy = this.exchange.marketBuy(symbol, qtyStr);
                return getTradeInfoFromOrder(marketBuy, basePrecision, quotePrecision, true);
            }
        }
        throw new BuyFailureException(symbol, priceStr, qtyStr, res.toString());
    }

    private TradeInfo secondRoundBuy(String symbol, double price, BigDecimal quoteQty) throws InterruptedException {
        Symbol symbolInfo = this.symbolMap.get(symbol);
        int basePrecision = symbolInfo.getAmountPrecision();
        int quotePrecision = symbolInfo.getPricePrecision();
        int biggerPrecision = basePrecision > quotePrecision ? basePrecision : quotePrecision;
        BigDecimal p = new BigDecimal(price).setScale(quotePrecision, RoundingMode.HALF_EVEN);
        BigDecimal qty = quoteQty
                .setScale(biggerPrecision, RoundingMode.DOWN)
                .divide(p, RoundingMode.DOWN)
                .setScale(basePrecision, RoundingMode.DOWN);

        String qtyStr = qty.toPlainString();
        String priceStr = p.toPlainString();
        OrderPlaceResponse res = this.exchange.limitBuy(symbol, qtyStr, priceStr);
        if (res.checkStatusOK()) {
            String orderId = res.getData();
            long record = System.currentTimeMillis();
            OrderDetail detail;
            for (;;) {
                detail = this.exchange.queryOrder(orderId);
                String state = detail.getState();
                if (FILLED.equals(state)) {
                    break;
                }
                if (System.currentTimeMillis() - record > 10000) {
                    double newBid = getTopBidPriceFromOrderBook(symbol);
                    if (newBid > 0 && newBid*BUY_SLIPPAGE > price){
                        detail = this.exchange.cancel(orderId);
                        break;
                    } else { //price has advantage, so wait more time
                        if (System.currentTimeMillis() - record > 100) {
                            detail = this.exchange.cancel(orderId);
                            break;
                        }
                    }
                    TimeUnit.MILLISECONDS.sleep(200);
                }
            }
            String state = detail.getState();
            if (FILLED.equals(state)) {
                return getTradeInfoFromOrder(detail, basePrecision, quotePrecision, true);
            } else if (CANCELED.equals(state)) {
                //market buy
                OrderDetail marketBuy = this.exchange.marketBuy(symbol, quoteQty.toPlainString());
                return getTradeInfoFromOrder(marketBuy, basePrecision, quotePrecision, true);
            } else if (PARTIAL_CANCELED.equals(state)) {
                TradeInfo filledTradeInfo = getTradeInfoFromOrder(detail, basePrecision, quotePrecision, true);
                BigDecimal filledBaseQty = filledTradeInfo.getQty();
                BigDecimal partQuoteQty = new BigDecimal(detail.getFieldCashAmount())
                        .setScale(quotePrecision, RoundingMode.UP);
                BigDecimal leftQuoteQty = quoteQty.subtract(partQuoteQty);
                //market buy
                OrderDetail marketBuy = this.exchange.marketBuy(symbol, leftQuoteQty.toPlainString());
                TradeInfo marketBuyTradeInfo = getTradeInfoFromOrder(marketBuy, basePrecision, quotePrecision, true);
                BigDecimal marketBuyBaseQty = new BigDecimal(marketBuy.getFieldAmount())
                        .setScale(basePrecision, RoundingMode.DOWN);
                BigDecimal totalBaseQty = filledBaseQty.add(marketBuyBaseQty);
                marketBuyTradeInfo.setQty(totalBaseQty);
                BigDecimal finalPrice = quoteQty
                        .setScale(biggerPrecision, RoundingMode.DOWN)
                        .divide(totalBaseQty, RoundingMode.DOWN)
                        .setScale(quotePrecision, RoundingMode.UP);
                marketBuyTradeInfo.setPrice(finalPrice);
                return marketBuyTradeInfo;
            }
        } else {
            log.error("Failed to buy. symbol: {}, qty: {} @price: {}, res: {}", symbol, qtyStr, priceStr, res);
//            return secondRoundBuy(symbol, price, quoteQty);
        }
        throw new OrderPlaceException(res.toString());
    }

    private TradeInfo sell(String symbol, double price, BigDecimal baseQty, long timeout) {
        Symbol symbolInfo = this.symbolMap.get(symbol);
        int basePrecision = symbolInfo.getAmountPrecision();
        int quotePrecision = symbolInfo.getPricePrecision();
        BigDecimal p = new BigDecimal(price).setScale(quotePrecision, RoundingMode.HALF_EVEN);
        String baseQtyStr = baseQty.setScale(basePrecision, RoundingMode.DOWN).toPlainString();
        String priceStr = p.toPlainString();
        OrderPlaceResponse res = this.exchange.limitSell(symbol, baseQtyStr, priceStr);
        if (res.checkStatusOK()) {
            String orderId = res.getData();
            long start = System.currentTimeMillis();
            for (;;) {
                OrderDetail detail = this.exchange.queryOrder(orderId);
                String state = detail.getState();
                if (FILLED.equals(state)) {
                    return getTradeInfoFromOrder(detail, basePrecision, quotePrecision, false);
                }
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    throw new SellFailureException(e);
                }
                if (System.currentTimeMillis() - start > timeout) {
                    break;
                }
            }
            OrderDetail cancel = this.exchange.cancel(orderId);
            BigDecimal soldQty = null;
            if (PARTIAL_CANCELED.equals(cancel.getState())) {
                BigDecimal partialBase = new BigDecimal(cancel.getFieldAmount())
                        .setScale(basePrecision, RoundingMode.DOWN);
                baseQty = baseQty.subtract(partialBase);
                soldQty = getQuoteQtyFromOrder(cancel, quotePrecision, RoundingMode.DOWN);
            }
            OrderDetail marketSell = this.exchange.marketSell(symbol, baseQtyStr);
            BigDecimal marketSellQuoteQty = getQuoteQtyFromOrder(marketSell, quotePrecision, RoundingMode.DOWN);
            TradeInfo tradeInfo = new TradeInfo();
            if (soldQty != null) {
                BigDecimal totalQuoteQty = marketSellQuoteQty.add(soldQty);
                BigDecimal finalPrice = totalQuoteQty.divide(baseQty, RoundingMode.DOWN)
                        .setScale(quotePrecision, RoundingMode.DOWN);
                tradeInfo.setPrice(finalPrice);
                tradeInfo.setQty(totalQuoteQty);
            } else {
                tradeInfo.setPrice(new BigDecimal(marketSell.getPrice()).setScale(quotePrecision, RoundingMode.DOWN));
                tradeInfo.setQty(marketSellQuoteQty);
            }
            return tradeInfo;
        }
        throw new SellFailureException(symbol, priceStr, baseQtyStr, res.toString());
    }

    private TradeInfo getTradeInfoFromOrder(OrderDetail detail, int basePrecision, int quotePrecision, boolean isBuy) {
        BigDecimal quotePrice = new BigDecimal(detail.getPrice())
                .setScale(quotePrecision, RoundingMode.UP);
        TradeInfo tradeInfo = new TradeInfo();
        tradeInfo.setPrice(quotePrice);
        BigDecimal totalQty;
        if (isBuy) {
            BigDecimal baseQty = new BigDecimal(detail.getFieldAmount()).setScale(basePrecision, RoundingMode.DOWN);
            BigDecimal fee = new BigDecimal(detail.getFieldFees()).setScale(basePrecision, RoundingMode.UP);
            totalQty = baseQty.subtract(fee);
        } else {
            BigDecimal quoteQty = getQuoteQtyFromOrder(detail, quotePrecision, RoundingMode.DOWN);
            BigDecimal fee = new BigDecimal(detail.getFieldFees()).setScale(quotePrecision, RoundingMode.UP);
            totalQty = quoteQty.subtract(fee);
        }
        tradeInfo.setQty(totalQty);

        return tradeInfo;
    }

    private BigDecimal getQuoteQtyFromOrder(OrderDetail detail, int quotePrecision, RoundingMode mode) {
        BigDecimal quote = new BigDecimal(detail.getFieldCashAmount())
                .setScale(quotePrecision, mode);
        BigDecimal fees = new BigDecimal(detail.getFieldFees())
                .setScale(quotePrecision, mode);
        return quote.subtract(fees);
    }

    private int getAskPriceLevelFromOrderBook(String symbol, double price) {
        Depth orderBook = this.exchange.getOrderBook(symbol);
        List<List<Double>> asks = orderBook.getAsks();
        if (asks != null && !asks.isEmpty()) {
            int i = 0;
            for (; i < asks.size(); i ++) {
                Double p = asks.get(i).get(0);
                if (price > p) {
                    return i;
                }
            }
            return i;
        }
        return -1;
    }

    private int getBidPriceLevelFromOrderBook(String symbol, double price) {
        Depth orderBook = this.exchange.getOrderBook(symbol);
        List<List<Double>> bids = orderBook.getBids();
        if (bids != null && !bids.isEmpty()) {
            int i = 0;
            for (; i < bids.size(); i ++) {
                Double p = bids.get(i).get(0);
                if (price < p) {
                    return i;
                }
            }
            return i;
        }
        return -1;
    }

    private double getTopBidPriceFromOrderBook(String symbol) {
        Depth orderBook = this.exchange.getOrderBook(symbol);
        List<List<Double>> bids = orderBook.getBids();
        if (bids != null && !bids.isEmpty()) {
            return bids.get(0).get(0);
        } else {
            return -1;
        }
    }

    private double getMeanBidBetweenPriceLevel(String symbol, int lvl1, int lvl2) {
        Depth orderBook = this.exchange.getOrderBook(symbol);
        List<List<Double>> bids = orderBook.getBids();
        if (bids.size() > lvl2) {
            Double p1 = bids.get(lvl1).get(0);
            Double p2 = bids.get(lvl2).get(0);
            return (p1+p2)/2;
        } else {
            return -1;
        }
    }

    private double getReverse(double source, double middle, double last) {
        return middle * source / last * TRIPLE_COMMISSION;
    }

    private double getClockwise(double source, double middle, double last) {
        return last / (source * middle) * TRIPLE_COMMISSION;
    }

    public static BigDecimal round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();

        BigDecimal bd = new BigDecimal(value);
        bd = bd.setScale(places, RoundingMode.DOWN);
        return bd;
    }

    public static void main(String[] args) {
        try {
            String accessKeyId = System.getenv("HUOBI_ACCESS_KEY");
            String accessKeySecret = System.getenv("HUOBI_ACCESS_KEY_SECRET");
            HuobiTriangleArbitrage strategy = new HuobiTriangleArbitrage("2672827", accessKeyId, accessKeySecret);
            strategy.init();
            strategy.run();
        } catch (Exception e) {
            log.error("Exception happened. Stop trading.", e);
        }
    }
}
