package io.magicalne.smym.strategy;

import io.magicalne.smym.dto.*;
import io.magicalne.smym.exception.OrderPlaceException;
import io.magicalne.smym.exchanges.HuobiExchange;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class HuobiTriangleArbitrage {
    private static final double COMMISSION = 0.998;
    private static final double TRIPLE_COMMISSION = COMMISSION*COMMISSION*COMMISSION;
    private static final int TIMEOUT_TRADE = 10000;
    private static final String PARTIAL_CANCELED = "partial-canceled";
    private static final String CANCELED = "canceled";
    private static final String PARTIAL_FILLED = "partial-filled";
    private static final String FILLED = "filled";
    private static final double UPPER_BOUND = 1.01;
    private static final double BUY_SLIPPAGE = 0.9992;
    private static final double SELL_SLIPPAGE = 1.0003;

    private final HuobiExchange exchange;
    private List<Triangular> btcusdtPairList;
    private List<Triangular> ethusdtPairList;
    private List<Triangular> htusdtPairList;
    private final Map<String, Symbol> symbolMap = new HashMap<>();

    private final String accountId;
    private String capital;


    public HuobiTriangleArbitrage(String accountId, String accessKey, String secretKey) {
        this.accountId = accountId;
        this.exchange = new HuobiExchange(accountId, accessKey, secretKey);
    }

    public void init() {
        List<Symbol> symbols = this.exchange.getSymbolInfo();
        for (Symbol s : symbols) {
            symbolMap.put(s.getSymbol(), s);
        }
//
//        Map<Symbol, Double> symbolWithVolume = new HashMap<>();
//        for (Symbol symbol : symbols) {
//            DetailResponse<Details> detail = this.client.detail(symbol.getSymbol());
//            Details tick = detail.getTick();
//            double vol = tick.getVol();
//            symbolWithVolume.put(symbol, vol);
//        }
//

        Map<String, List<Symbol>> quoteGroup =
                symbols.stream().collect(Collectors.groupingBy(Symbol::getQuoteCurrency));

        List<Symbol> usdtGrp = quoteGroup.get("usdt");
        List<Symbol> btcGrp = quoteGroup.get("btc");
        List<Symbol> ethGrp = quoteGroup.get("eth");
        List<Symbol> htGrp = quoteGroup.get("ht");

        //usdt with btc
        List<Triangular> btcusdtPairList = new LinkedList<>();
        String btcusdt = "btcusdt";
        List<Triangular> ethusdtPairList = new LinkedList<>();
        String ethusdt = "ethusdt";
        List<Triangular> htusdtPairList = new LinkedList<>();
        String htusdt = "htusdt";
        for (Symbol u : usdtGrp) {
//            for (Symbol b : btcGrp) {
//                if (u.getBaseCurrency().equals(b.getBaseCurrency())) {
//                    Triangular triangular = new Triangular(btcusdt, b.getBaseCurrency() + "btc", b.getBaseCurrency() + "usdt");
//                    btcusdtPairList.add(triangular);
//                }
//            }

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
        htusdtPairList.forEach(t -> {
            symbolSet.add(t.getSource());
            symbolSet.add(t.getMiddle());
            symbolSet.add(t.getLast());
        });
        this.htusdtPairList = htusdtPairList;

        exchange.createOrderBook(symbolSet, 5);
    }

    public String getCapitalFromBalance(String currency) {
        BalanceResponse balanceRes = this.exchange.getAccount(accountId);
        List<BalanceBean> balances = balanceRes.getData().getList();
        for (BalanceBean b : balances) {
            if (currency.equals(b.getCurrency())) {
                return b.getBalance();
            }
        }
        return null;
    }

    private void run(String capital) {
        double c = Double.parseDouble(capital);

        String balance = getCapitalFromBalance("usdt");
        double b = Double.parseDouble(balance);
        this.capital = c > b ? balance : capital;
        for (;;) {
            findArbitrage(this.btcusdtPairList);
            findArbitrage(this.ethusdtPairList);
            findArbitrage(this.htusdtPairList);
        }
    }

    private void findArbitrage(List<Triangular> pairList) {
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
                try {
                    log.info("Use {}st price in order book. Clockwise, {}: {} -> {}: {} -> {}: {}, profit: {}",
                            priceLevel+1,
                            triangular.getSource(), source,
                            triangular.getMiddle(), middle,
                            triangular.getLast(), last,
                            profit);
                    takeIt(triangular, source, middle, last, this.capital, true);
                } catch (InterruptedException e) {
                    log.error("InterruptedException.", e);
                }
            }
            //reverse clockwise
            List<List<Double>> sourceDepthBids = sourceDepth.getBids();
            List<List<Double>> middleDepthBids = middleDepth.getBids();
            List<List<Double>> lastDepthAsks = lastDepth.getAsks();
            source = sourceDepthBids.get(priceLevel).get(0);
            middle = middleDepthBids.get(priceLevel).get(0);
            last = lastDepthAsks.get(priceLevel).get(0);
            profit = getReverse(source * SELL_SLIPPAGE, middle * SELL_SLIPPAGE, last * BUY_SLIPPAGE);
            if (profit > UPPER_BOUND) {
                log.info("Use {}st price in order book. Reverse Clockwise, {}: {} -> {}: {} -> {}: {}, profit: {}",
                        priceLevel+1,
                        triangular.getSource(), source,
                        triangular.getMiddle(), middle,
                        triangular.getLast(), last,
                        profit);
            }
        }
    }

    public void takeIt(Triangular triangular, double sourcePrice, double middlePrice, double lastPrice,
                        String quoteQty, boolean clockwise) throws InterruptedException {

        if (clockwise) {
            TradeInfo firstRound = null;
            try {
                firstRound = firstRoundBuy(triangular.getSource(), sourcePrice, quoteQty);
            } catch (Exception e) {
                log.error("Exception happened in first round. Buy:{}@{} qty:{}. {}",
                        triangular.getSource(), sourcePrice, quoteQty, e);
            }
            if (firstRound == null) {
                return;
            }
            TradeInfo secondRound = secondRoundBuy(triangular.getMiddle(), middlePrice, firstRound.getQty());
            TradeInfo finalQuoteQty = thirdRoundSell(triangular.getLast(), lastPrice, secondRound.getQty());
            BigDecimal finalQty = finalQuoteQty.getQty();
            BigDecimal profit = finalQty.divide(new BigDecimal(quoteQty), 5);
            log.info("Actually, the return of this round: {}", profit.toPlainString());
        }
    }

    private TradeInfo firstRoundBuy(String symbol, double price, String quoteQty) {
        Symbol symbolInfo = this.symbolMap.get(symbol);
        int basePrecision = symbolInfo.getAmountPrecision();
        int quotePrecision = symbolInfo.getPricePrecision();
        BigDecimal p = new BigDecimal(price).setScale(quotePrecision, RoundingMode.DOWN);
        BigDecimal q = new BigDecimal(quoteQty).setScale(quotePrecision, RoundingMode.DOWN);
        BigDecimal qty = q.divide(p, basePrecision);

        OrderPlaceResponse res = this.exchange.limitBuy(symbol, qty.toPlainString(), p.toPlainString());
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
            }
            if (FILLED.equals(detail.getState()) || PARTIAL_CANCELED.equals(detail.getState())) {
                return getTradeInfoFromOrder(detail, basePrecision, quotePrecision, true);
            }
        }
        return null;
    }

    private TradeInfo secondRoundBuy(String symbol, double price, BigDecimal quoteQty) throws InterruptedException {
        Symbol symbolInfo = this.symbolMap.get(symbol);
        int basePrecision = symbolInfo.getAmountPrecision();
        int quotePrecision = symbolInfo.getPricePrecision();
        BigDecimal p = new BigDecimal(price).setScale(quotePrecision, RoundingMode.DOWN);
        BigDecimal qty = quoteQty.divide(p, basePrecision);

        OrderPlaceResponse res = this.exchange.limitBuy(symbol, qty.toPlainString(), p.toPlainString());
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
                if (System.currentTimeMillis() - record > 5000) {
                    double newBid = getTopBidPriceFromOrderBook(symbol);
                    if (newBid > 0 && newBid*BUY_SLIPPAGE > price){
                        detail = this.exchange.cancel(orderId);
                        break;
                    } else { //price has advantage, so wait more time
                        if (System.currentTimeMillis() - record > 10) {
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
                BigDecimal filledPart = new BigDecimal(detail.getFieldAmount())
                        .setScale(quotePrecision, RoundingMode.DOWN);
                BigDecimal partQuoteQty = new BigDecimal(detail.getFieldCashAmount())
                        .setScale(quotePrecision, RoundingMode.DOWN);
                BigDecimal fees = new BigDecimal(detail.getFieldFees())
                        .setScale(quotePrecision, RoundingMode.DOWN);
                BigDecimal leftQuoteQty = quoteQty.subtract(partQuoteQty).subtract(fees);
                //market buy
                OrderDetail marketBuy = this.exchange.marketBuy(symbol, leftQuoteQty.toPlainString());
                TradeInfo marketBuyTradeInfo = getTradeInfoFromOrder(marketBuy, basePrecision, quotePrecision, true);
                BigDecimal marketBuyBaseQty = new BigDecimal(marketBuy.getFieldAmount())
                        .setScale(basePrecision, RoundingMode.DOWN);
                BigDecimal totalBaseQty = filledPart.add(marketBuyBaseQty);
                marketBuyTradeInfo.setQty(totalBaseQty);
                BigDecimal finalPrice = quoteQty.divide(totalBaseQty, quotePrecision);
                marketBuyTradeInfo.setPrice(finalPrice);
                return marketBuyTradeInfo;
            }
        } else {
            return secondRoundBuy(symbol, price, quoteQty);
        }
        throw new OrderPlaceException(res.toString());
    }

    private TradeInfo thirdRoundSell(String symbol, double price, BigDecimal baseQty) throws InterruptedException {
        Symbol symbolInfo = this.symbolMap.get(symbol);
        int basePrecision = symbolInfo.getAmountPrecision();
        int quotePrecision = symbolInfo.getPricePrecision();
        BigDecimal p = new BigDecimal(price).setScale(quotePrecision, RoundingMode.DOWN);
        OrderPlaceResponse res = this.exchange.limitSell(symbol, baseQty.toPlainString(), p.toPlainString());
        if (res.checkStatusOK()) {
            String orderId = res.getData();
            long start = System.currentTimeMillis();
            for (;;) {
                OrderDetail detail = this.exchange.queryOrder(orderId);
                String state = detail.getState();
                if (FILLED.equals(state)) {
                    return getTradeInfoFromOrder(detail, basePrecision, quotePrecision, false);
                }
                TimeUnit.SECONDS.sleep(1);
                if (System.currentTimeMillis() - start > 600000) {//10min
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
            OrderDetail marketSell = this.exchange.marketSell(symbol, baseQty.toPlainString());
            BigDecimal marketSellQuoteQty = getQuoteQtyFromOrder(marketSell, quotePrecision, RoundingMode.DOWN);
            TradeInfo tradeInfo = new TradeInfo();
            if (soldQty != null) {
                BigDecimal totalQuoteQty = marketSellQuoteQty.add(soldQty);
                BigDecimal finalPrice = totalQuoteQty.divide(baseQty, quotePrecision);
                tradeInfo.setPrice(finalPrice);
                tradeInfo.setQty(totalQuoteQty);
            } else {
                tradeInfo.setPrice(new BigDecimal(marketSell.getPrice()).setScale(quotePrecision, RoundingMode.DOWN));
                tradeInfo.setQty(marketSellQuoteQty);
            }
            return tradeInfo;

        } else {
            return thirdRoundSell(symbol, price, baseQty);
        }
    }

    private TradeInfo getTradeInfoFromOrder(OrderDetail detail, int basePrecision, int quotePrecision, boolean isBuy) {
        BigDecimal quotePrice = new BigDecimal(detail.getPrice())
                .setScale(quotePrecision, RoundingMode.UP);
        TradeInfo tradeInfo = new TradeInfo();
        tradeInfo.setPrice(quotePrice);
        if (isBuy) {
            BigDecimal baseQty = new BigDecimal(detail.getFieldAmount()).setScale(basePrecision, RoundingMode.DOWN);
            tradeInfo.setQty(baseQty);
        } else {
            BigDecimal quoteQty = getQuoteQtyFromOrder(detail, quotePrecision, RoundingMode.DOWN);
            tradeInfo.setQty(quoteQty);
        }
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
            strategy.run(args[0]);
        } catch (Exception e) {
            log.error("Exception happened. Stop trading.", e);
        }
    }
}
