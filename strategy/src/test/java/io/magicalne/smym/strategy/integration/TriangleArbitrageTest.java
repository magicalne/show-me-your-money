package io.magicalne.smym.strategy.integration;

import io.magicalne.smym.dto.Depth;
import io.magicalne.smym.dto.Symbol;
import io.magicalne.smym.exchanges.HuobiExchange;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TriangleArbitrageTest {

    public static final String ACCOUNT_ID = "2672827";
    private HuobiExchange huobiExchange;
    @Before
    public void setup() {
        String accessKeyId = System.getenv("HUOBI_ACCESS_KEY");
        String accessKeySecret = System.getenv("HUOBI_ACCESS_KEY_SECRET");
        huobiExchange = new HuobiExchange(ACCOUNT_ID, accessKeyId, accessKeySecret);
    }

    @Test
    public void test1() {
        List<Symbol> symbolInfo = huobiExchange.getSymbolInfo();
        for (Symbol symbol : symbolInfo) {
            if ("ethusdt".equals(symbol.getSymbol())) {
                System.out.println(symbol);
                break;
            }
        }
    }

    @Test
    public void test2() {
        int basePrecision = 4;
        int quotePrecision = 2;
        BigDecimal q = new BigDecimal("2").setScale(basePrecision, RoundingMode.DOWN);
        BigDecimal p = new BigDecimal(2342.45).setScale(quotePrecision, RoundingMode.HALF_EVEN);
        BigDecimal qty = q.divide(p, RoundingMode.HALF_EVEN).setScale(basePrecision, RoundingMode.DOWN);

        String qtyStr = qty.toPlainString();
        String priceStr = p.toPlainString();
        System.out.println(qtyStr);
        System.out.println(priceStr);
    }

    @Test
    public void test9() throws InterruptedException {

        Set<String> symbols = new HashSet<>();
        symbols.add("leteth");
        huobiExchange.createOrderBook(symbols, 5);

        for (;;) {
            Depth depth = huobiExchange.getOrderBook("leteth");
            if (depth == null) {
                continue;
            }
            List<List<Double>> asks = depth.getAsks();
            System.out.println("ask################");
            asks.forEach(e -> System.out.println("price:" + e.get(0) + ", qty:" + e.get(1)));
            System.out.println("bid################");
            List<List<Double>> bids = depth.getBids();
            bids.forEach(e -> System.out.println("price:" + e.get(0) + ", qty:" + e.get(1)));
            Thread.sleep(5000);

        }
    }
}
