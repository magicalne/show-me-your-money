package io.magicalne.smym.strategy;

import io.magicalne.smym.dto.*;
import io.magicalne.smym.exception.CancelOrderException;
import io.magicalne.smym.exception.OrderPlaceException;
import io.magicalne.smym.exchanges.HuobiProRest;
import io.magicalne.smym.exchanges.HuobiProSubKlineWebSocket;
import io.magicalne.smym.handler.SubEventHandler;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class HuobiTriangleArbitrage {
    private static final double COMMISSION = 0.998;
    private static final double TRIPLE_COMMISSION = COMMISSION*COMMISSION*COMMISSION;
    private static final int TIMEOUT_TRADE = 10000;
    private static final String PARTIAL_CANCELED = "partial-canceled";
    private static final String CANCELED = "canceled";
    private static final String FILLED = "filled";
    private final double buySlippage = 0.9998;
    private final double sellSlippage = 1.0001;

    private final HuobiProRest client;

    private SubEventHandler subEventHandler;
    private List<Triangular> btcusdtPairList;
    private List<Triangular> ethusdtPairList;
    private List<Triangular> htusdtPairList;
    private final Map<String, Symbol> symbolMap = new HashMap<>();

    private final OrderPlaceRequest sourceReq = new OrderPlaceRequest();
    private final OrderPlaceRequest middleReq = new OrderPlaceRequest();
    private final OrderPlaceRequest lastReq = new OrderPlaceRequest();
    private final String accountId;

    public HuobiTriangleArbitrage(String accountId) {
        this.accountId = accountId;
        this.sourceReq.setAccountId(accountId);
        this.middleReq.setAccountId(accountId);
        this.lastReq.setAccountId(accountId);
        this.client = new HuobiProRest("fb5335bd-b1902e32-cc0b36e2-6850a", "49761a11-c8c3b8d3-e3639666-6a6c4");
    }

    public void init() {
        List<Symbol> symbols = this.client.getSymbols();
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

            for (Symbol h : htGrp) {
                if (u.getBaseCurrency().equals(h.getBaseCurrency())) {
                    Triangular triangular = new Triangular(htusdt, h.getBaseCurrency() + "ht", h.getBaseCurrency() + "usdt");
                    htusdtPairList.add(triangular);
                }
            }
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

        subEventHandler = new SubEventHandler(symbolSet.size());
        HuobiProSubKlineWebSocket websocket = new HuobiProSubKlineWebSocket(symbolSet, "1min", subEventHandler);
        websocket.init();
    }

    public double getCapitalFromBalance(String currency) {
        BalanceResponse balanceRes = client.balance(accountId);
        List<BalanceBean> balances = balanceRes.getData().getList();
        double balance = -1;
        for (BalanceBean b : balances) {
            if (currency.equals(b.getCurrency())) {
                balance = Double.parseDouble(b.getBalance());
                log.info("Balance USDT: {}", balance);
                break;
            }
        }
        return balance;

    }

    private void run(String capital, boolean isTest) throws CancelOrderException, OrderPlaceException {
        double capitalDouble = Double.parseDouble(capital);

        capitalDouble = Math.min(capitalDouble, getCapitalFromBalance("usdt"));
        double minVol = 7d;
        Map<String, SubKlineRes> sub = subEventHandler.getSub();
        for (;;) {
            for (Triangular triangular : this.btcusdtPairList) {
                if (!sub.containsKey(triangular.getSource())
                        || !sub.containsKey(triangular.getMiddle())
                        || !sub.containsKey(triangular.getLast())) {
                    continue;
                }
                capitalDouble = strategy(isTest, capitalDouble, minVol, triangular);
            }
            for (Triangular triangular : this.ethusdtPairList) {
                if (!sub.containsKey(triangular.getSource())
                        || !sub.containsKey(triangular.getMiddle())
                        || !sub.containsKey(triangular.getLast())) {
                    continue;
                }
                capitalDouble = strategy(isTest, capitalDouble, minVol, triangular);
            }
            for (Triangular triangular : this.htusdtPairList) {
                if (!sub.containsKey(triangular.getSource())
                        || !sub.containsKey(triangular.getMiddle())
                        || !sub.containsKey(triangular.getLast())) {
                    continue;
                }
                capitalDouble = strategy(isTest, capitalDouble, minVol, triangular);
            }
        }
    }

    private double strategy(boolean isTest, double capitalDouble, double minVol, Triangular triangular)
            throws CancelOrderException, OrderPlaceException {
        double upperBound = 1.01;
        double underBound = 0.98;
        Map<String, SubKlineRes> sub = subEventHandler.getSub();
        double source = sub.get(triangular.getSource()).getTick().getClose();
        double middle = sub.get(triangular.getMiddle()).getTick().getClose();
        double last = sub.get(triangular.getLast()).getTick().getClose();
        double lastAmount = sub.get(triangular.getLast()).getTick().getAmount();
        double lastVol = sub.get(triangular.getLast()).getTick().getVol();
        double diff = last / (source * middle) * TRIPLE_COMMISSION;
        if (lastVol > minVol && (diff >= upperBound || diff <= underBound)) {
            sub.remove(triangular.getSource());
            sub.remove(triangular.getMiddle());
            sub.remove(triangular.getLast());
            log.info("{} : source: {}, middle: {}, last: {}, diff: {}, last amount: {}, last vol: {}",
                    triangular, source, middle, last, diff, lastAmount, lastVol);
            boolean clockwise = diff >= upperBound;
            if (isTest) {
                capitalDouble = orderTest(capitalDouble, source, middle, last, lastVol, clockwise);
            } else {
                capitalDouble = order(capitalDouble < lastVol ? capitalDouble : lastVol,
                        triangular.getSource(), triangular.getMiddle(), triangular.getLast(),
                        source, middle, last, clockwise);
                capitalDouble = Math.min(capitalDouble, getCapitalFromBalance("usdt"));
            }
        }
        return capitalDouble;
    }

    private double order(double capital, String source, String middle, String last,
                         double sourcePrice, double middlePrice, double lastPrice, boolean clockwise)
            throws CancelOrderException, OrderPlaceException {
        if (clockwise) {
            log.info("Clockwise trade.");
            double sourceAmount = buyLimit(sourceReq, capital, sourcePrice, source);
            double middleAmount = buyLimit(middleReq, sourceAmount, middlePrice, middle);
            double lastAmount = sellLimit(lastReq, middleAmount, lastPrice, last);
            log.info("Capital was {}, now capital is : {}, profit: {}", capital, lastAmount, lastAmount-capital);
            return lastAmount;
        } else {
            log.info("Reverse clockwise trade.");
            double lastAmount = buyLimit(lastReq, capital, lastPrice, last);
            double middleAmount = sellLimit(middleReq, lastAmount, middlePrice, middle);
            double sourceAmount = sellLimit(sourceReq, middleAmount, sourcePrice, source);
            log.info("Capital was {}, now capital is : {}, profit: {}", capital, sourceAmount, sourceAmount-capital);
            return sourceAmount;
        }
    }

    public double buyLimit(OrderPlaceRequest req, double capital, double price, String symbol)
            throws OrderPlaceException, CancelOrderException {
        Symbol s = symbolMap.get(symbol);
        int amountPrecision = s.getAmountPrecision();
        int pricePrecision = s.getPricePrecision();

        BigDecimal slippagePrice = round(price*sellSlippage, pricePrecision);

        BigDecimal amount = round(capital/slippagePrice.doubleValue(), amountPrecision);

        req.setAmount(amount.toPlainString());
        req.setPrice(slippagePrice.toPlainString());
        req.setSymbol(symbol);
        req.setType(OrderType.BUY_LIMIT.getType());

        OrderPlaceResponse res = this.client.orderPlace(req);
        for (int i = 0; i < 3; i ++) {
            if (res.checkStatusOK()) {
                String orderId = res.getData();
                long start = System.currentTimeMillis();
                long roundStart = start;
                for (;;) {
                    long round = System.currentTimeMillis();
                    if (round - roundStart < 500) {
                        continue;
                    } else {
                        roundStart = round;
                    }
                    OrdersDetailResponse ordersDetailResponse = this.client.ordersDetail(orderId);
                    if (ordersDetailResponse.checkStatusOK()) {
                        OrderDetail detail = ordersDetailResponse.getData();
                        String state = detail.getState();
                        if (FILLED.equals(state)) {
                            double baseAmount = Double.parseDouble(detail.getFieldAmount());
                            double fee = Double.parseDouble(detail.getFieldFees());
                            return baseAmount - fee;
                        } else {
                            //buy market
                            if (System.currentTimeMillis() - start > TIMEOUT_TRADE) {
                                OrderDetail cancel = cancel(orderId);
                                if (PARTIAL_CANCELED.equals(cancel.getState())) {
                                    double partialAmount = Double.parseDouble(cancel.getFieldAmount());
                                    double quote = Double.parseDouble(cancel.getFieldCashAmount());
                                    double partialFee = Double.parseDouble(cancel.getFieldFees());

                                    BigDecimal capLeft = round(capital - quote, pricePrecision);
                                    return buyMarket(req, capLeft.toPlainString(), symbol) + partialAmount - partialFee;
                                } else {
                                    return buyMarket(req, round(capital, pricePrecision).toPlainString(), symbol);
                                }
                            }
                        }
                    }
                }
            }
        }
        throw new OrderPlaceException(req.toString());
    }

    public static BigDecimal round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();

        BigDecimal bd = new BigDecimal(value);
        bd = bd.setScale(places, RoundingMode.DOWN);
        return bd;
    }

    private double buyMarket(OrderPlaceRequest req, String capital, String symbol) throws OrderPlaceException {

        req.setAmount(capital);
        req.setPrice(null);
        req.setType(OrderType.BUY_MARKET.getType());
        req.setSymbol(symbol);
        OrderPlaceResponse orderPlaceResponse = this.client.orderPlace(req);
        for (int i = 0; i < 3; i ++) {
            if (orderPlaceResponse.checkStatusOK()) {
                long start = System.currentTimeMillis();
                for (; ; ) {
                    long round = System.currentTimeMillis();
                    if (round - start < 500) {
                        continue;
                    } else {
                        start = round;
                    }
                    String orderId = orderPlaceResponse.getData();
                    OrdersDetailResponse order = this.client.ordersDetail(orderId);
                    if (order.checkStatusOK()) {
                        OrderDetail detail = order.getData();
                        if (FILLED.equals(detail.getState())) {
                            log.info("buy market: {}", detail);
                            double fieldAmount = Double.parseDouble(detail.getFieldAmount());
                            double fee = Double.parseDouble(detail.getFieldFees());
                            return fieldAmount - fee;
                        }
                    }
                }
            }
        }
        throw new OrderPlaceException(req.toString());
    }

    public double sellLimit(OrderPlaceRequest req, double capital, double price, String symbol)
            throws OrderPlaceException, CancelOrderException {
        Symbol s = symbolMap.get(symbol);
        int amountPrecision = s.getAmountPrecision();
        int pricePrecision = s.getPricePrecision();

        BigDecimal slippagePrice = round(price*buySlippage, pricePrecision);


        BigDecimal capitalBD = round(capital, amountPrecision);
        req.setAmount(capitalBD.toPlainString());
        req.setPrice(slippagePrice.toPlainString());
        req.setSymbol(symbol);
        req.setType(OrderType.SELL_LIMIT.getType());

        OrderPlaceResponse res = this.client.orderPlace(req);
        for (int i = 0; i < 3; i ++) {
            if (res.checkStatusOK()) {
                String orderId = res.getData();
                long start = System.currentTimeMillis();
                long roundStart = start;
                for (; ; ) {
                    long round = System.currentTimeMillis();
                    if (round - roundStart < 500) {
                        continue;
                    } else {
                        roundStart = round;
                    }
                    OrdersDetailResponse ordersDetailResponse = this.client.ordersDetail(orderId);
                    if (ordersDetailResponse.checkStatusOK()) {
                        OrderDetail detail = ordersDetailResponse.getData();
                        String state = detail.getState();
                        if (FILLED.equals(state)) {
                            double quote = Double.parseDouble(detail.getFieldCashAmount());
                            double fee = Double.parseDouble(detail.getFieldFees());
                            return quote - fee;
                        } else {
                            if (System.currentTimeMillis() - start > TIMEOUT_TRADE) {
                                OrderDetail cancel = cancel(orderId);
                                if (PARTIAL_CANCELED.equals(cancel.getState())) {

                                    double partialAmount = Double.parseDouble(cancel.getFieldCashAmount());
                                    double base = Double.parseDouble(cancel.getFieldAmount());
                                    double partialFee = Double.parseDouble(cancel.getFieldFees());

                                    BigDecimal capLeft = round(capitalBD.doubleValue() - base, amountPrecision);
                                    return sellMarket(req, capLeft.toPlainString(), symbol) + partialAmount - partialFee;

                                } else {
                                    return sellMarket(req, round(capital, amountPrecision).toPlainString(), symbol);
                                }
                            }
                        }
                    }
                }
            }
        }
        throw new OrderPlaceException(req.toString());
    }

    private double sellMarket(OrderPlaceRequest req, String capital, String symbol) throws OrderPlaceException {
        req.setAmount(capital);
        req.setPrice(null);
        req.setType(OrderType.SELL_MARKET.getType());
        req.setSymbol(symbol);
        OrderPlaceResponse orderPlaceResponse = this.client.orderPlace(req);
        for (int i = 0; i < 3; i ++) {
            if (orderPlaceResponse.checkStatusOK()) {
                long start = System.currentTimeMillis();
                for (; ; ) {
                    long round = System.currentTimeMillis();
                    if (round - start < 500) {
                        continue;
                    } else {
                        start = round;
                    }
                    String orderId = orderPlaceResponse.getData();
                    OrdersDetailResponse order = this.client.ordersDetail(orderId);

                    if (order.checkStatusOK()) {
                        OrderDetail detail = order.getData();
                        if (FILLED.equals(detail.getState())) {
                            log.info("sell market: {}", detail);
                            double fieldAmount = Double.parseDouble(detail.getFieldCashAmount());
                            double fee = Double.parseDouble(detail.getFieldFees());
                            return fieldAmount - fee;
                        }
                    }
                }
            }
        }
        throw new OrderPlaceException(req.toString());
    }

    private OrderDetail cancel(String orderId) throws CancelOrderException {
        SubmitCancelResponse cancel = this.client.submitcancel(orderId);
        if (cancel.checkStatusOK()) {
            long start = System.currentTimeMillis();
            for (;;) {
                long round = System.currentTimeMillis();
                if (round - start < 500) {
                    continue;
                } else {
                    start = round;
                }
                OrdersDetailResponse ordersDetailResponse = this.client.ordersDetail(orderId);
                if (ordersDetailResponse.checkStatusOK()) {
                    OrderDetail detail = ordersDetailResponse.getData();
                    String state = detail.getState();
                    if (PARTIAL_CANCELED.equals(state) || CANCELED.equals(state)) {
                        log.info("cancel: {}", ordersDetailResponse);
                        return detail;
                    }
                }
            }
        }
        throw new CancelOrderException(orderId);
    }

    double orderTest(double capital,
                     double source,
                     double middle,
                     double last,
                     double lastVol,
                     boolean clockwise) {
        double c = capital;
        double amount;
        if (lastVol < c) {
            amount = lastVol;
        } else {
            amount = c;
        }
        double result;
        //amount / source / middle * last * 0.998^3
        if (clockwise) {
            result = amount/(source*buySlippage*middle*buySlippage)*last*sellSlippage*TRIPLE_COMMISSION;
        } else {
            result = amount/last*buySlippage*middle*sellSlippage*source*sellSlippage*TRIPLE_COMMISSION;
        }
        double profit = result - amount;
        c = capital + profit;
        log.info("Init capital: {}. The profit of this round trade: {}, total capital: {}, return rate: {}",
                capital, profit, c, profit/capital);
        return c;
    }

    public static void main(String[] args) {
        try {
            HuobiTriangleArbitrage strategy = new HuobiTriangleArbitrage("2672827");
            strategy.init();
            strategy.run(args[0], Boolean.parseBoolean(args[1]));
        } catch (Exception e) {
            log.info("Exception happened. Stop trading.", e);
        }
    }
}
