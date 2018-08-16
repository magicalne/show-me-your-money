package io.magicalne.smym.exchanges.huobi;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Strings;
import io.magicalne.smym.Utils;
import io.magicalne.smym.dto.*;
import io.magicalne.smym.exception.ApiException;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.Proxy;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class HuobiProRest {

    private static final int CONN_TIMEOUT = 5;
    private static final int READ_TIMEOUT = 5;
    private static final int WRITE_TIMEOUT = 5;

    private static final String API_HOST = "api.huobi.pro";

    private static final String API_URL = "https://" + API_HOST;
    private static final MediaType JSON = MediaType.parse("application/json");
    private static final OkHttpClient client = createOkHttpClient();
    public static final String POST = "POST";
    public static final String HMAC_SHA_256 = "HmacSHA256";

    private final String accessKeyId;
    private final String accessKeySecret;
    private final String assetPassword;

    public HuobiProRest(String accessKey, String accessKeySecret) {
        this.accessKeyId = accessKey;
        this.accessKeySecret = accessKeySecret;
        this.assetPassword = null;
    }

    public HuobiProRest(String accessKeyId, String accessKeySecret, String assetPassword) {
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
        this.assetPassword = assetPassword;
    }

    /**
     * 查询交易对
     *
     * @return List of symbols.
     */
    public List<Symbol> getSymbols() {
        ApiResponse<List<Symbol>> resp =
                get("/v1/common/symbols", null, new TypeReference<ApiResponse<List<Symbol>>>() {
                });
        return resp.checkAndReturn();
    }

    /**
     * 查询所有账户信息
     *
     * @return List of accounts.
     */
    public List<Account> getAccounts() {
        ApiResponse<List<Account>> resp =
                get("/v1/account/accounts", null, new TypeReference<ApiResponse<List<Account>>>() {
                });
        return resp.checkAndReturn();
    }

    /**
     * 创建订单（未执行)
     *
     * @param request CreateOrderRequest object.
     * @return Order id.
     */
    public Long createOrder(CreateOrderRequest request) {
        ApiResponse<Long> resp =
                post("/v1/order/orders", request, new TypeReference<ApiResponse<Long>>() {
                });
        return resp.checkAndReturn();
    }

    /**
     * 执行订单
     *
     * @param orderId The id of created order.
     * @return Order id.
     */
    public String placeOrder(long orderId) {
        ApiResponse<String> resp = post("/v1/order/orders/" + orderId + "/place", null,
                new TypeReference<ApiResponse<String>>() {
                });
        return resp.checkAndReturn();
    }

    public OrderPlaceResponse orderPlace(OrderPlaceRequest req) {
        return post("/v1/order/orders/place", req, new TypeReference<OrderPlaceResponse>() {});
    }


    // ----------------------------------------行情API-------------------------------------------

    /**
     * GET /market/history/kline 获取K线数据
     *
     * @param symbol
     * @param period
     * @param size
     * @return
     */
    public KlineResponse kline(String symbol, String period, String size) {
        Map<String, String> map = new HashMap<>();
        map.put("symbol", symbol);
        map.put("period", period);
        map.put("size", size);
        return get("/market/history/kline", map, new TypeReference<KlineResponse<List<Kline>>>() {
        });
    }

    /**
     * GET /market/detail/merged 获取聚合行情(Ticker)
     *
     * @param symbol
     * @return
     */
    public MergedResponse merged(String symbol) {
        HashMap map = new HashMap<>();
        map.put("symbol", symbol);
        MergedResponse resp = get("/market/detail/merged", map, new TypeReference<MergedResponse<List<Merged>>>() {
        });
        return resp;
    }

    /**
     * GET /market/depth 获取 Market Depth 数据
     *
     * @param request
     * @return
     */
    public DepthResponse depth(DepthRequest request) {
        Map<String, String> map = new HashMap<>();
        map.put("symbol", request.getSymbol());
        map.put("type", request.getType());

        return get("/market/depth", map, new TypeReference<DepthResponse>() {});
    }

    /**
     * GET /market/trade 获取 Trade Detail 数据
     *
     * @param symbol
     * @return
     */
    public TradeResponse trade(String symbol) {
        Map<String, String> map = new HashMap<>();
        map.put("symbol", symbol);
        TradeResponse resp = get("/market/trade", map, new TypeReference<TradeResponse>() {
        });
        return resp;
    }

    /**
     * GET /market/history/trade 批量获取最近的交易记录
     *
     * @param symbol
     * @param size
     * @return
     */
    public HistoryTradeResponse historyTrade(String symbol, String size) {
        Map<String, String> map = new HashMap<>();
        map.put("symbol", symbol);
        map.put("size", size);
        return get("/market/history/trade", map, new TypeReference<HistoryTradeResponse>() {
        });
    }

    /**
     * GET /market/detail 获取 Market Detail 24小时成交量数据
     *
     * @param symbol
     * @return
     */
    public DetailResponse<Details> detail(String symbol) {
        Map<String, String> map = new HashMap<>();
        map.put("symbol", symbol);
        return get("/market/detail", map, new TypeReference<DetailResponse<Details>>() {});
    }

    /**
     * GET /v1/common/currencys 查询系统支持的所有币种
     *
     * @param symbol
     * @return
     */
    public CurrencysResponse currencys(String symbol) {
        Map<String, String> map = new HashMap<>();
        map.put("symbol", symbol);
        return get("/v1/common/currencys", map, new TypeReference<CurrencysResponse>() {});
    }

    /**
     * GET /v1/common/timestamp 查询系统当前时间
     *
     * @return
     */
    public TimestampResponse timestamp() {
        return get("/v1/common/timestamp", null, new TypeReference<TimestampResponse>() {});
    }

    /**
     * GET /v1/account/accounts 查询当前用户的所有账户(即account-id)
     *
     * @return
     */
    public AccountsResponse accounts() {
        return get("/v1/account/accounts", null, new TypeReference<AccountsResponse>() {});
    }

    /**
     * GET /v1/account/accounts/{account-id}/balance 查询指定账户的余额
     *
     * @param accountId
     * @return
     */
    public BalanceResponse balance(String accountId) {
        return get("/v1/account/accounts/" + accountId + "/balance", null,
                new TypeReference<BalanceResponse>() {});
    }

    /**
     * POST /v1/order/orders/{order-id}/submitcancel 申请撤销一个订单请求
     *
     * @param orderId
     * @return
     */
    public SubmitCancelResponse submitcancel(String orderId) {
        SubmitCancelResponse resp = post("/v1/order/orders/" + orderId + "/submitcancel", null,
                new TypeReference<SubmitCancelResponse>() {
        });
        return resp;
    }

    /**
     * POST /v1/order/orders/batchcancel 批量撤销订单
     *
     * @param orderList
     * @return
     */
    public BatchcancelResponse submitcancels(List orderList) {
        Map<String, List> parameterMap = new HashMap<>();
        parameterMap.put("order-ids", orderList);
        return post("/v1/order/orders/batchcancel", parameterMap,
                new TypeReference<BatchcancelResponse>() {});
    }

    /**
     * GET /v1/order/orders/{order-id} 查询某个订单详情
     *
     * @param orderId
     * @return
     */
    public OrdersDetailResponse ordersDetail(String orderId) {
        return get("/v1/order/orders/" + orderId, null, new TypeReference<OrdersDetailResponse>() {});
    }


    /**
     * GET /v1/order/orders/{order-id}/matchresults 查询某个订单的成交明细
     *
     * @param orderId
     * @return
     */
    public MatchresultsOrdersDetailResponse matchresults(String orderId) {
        return get("/v1/order/orders/" + orderId + "/matchresults", null,
                new TypeReference<MatchresultsOrdersDetailResponse>() {});
    }
    
    public ApiResponse<List<OrderDetail>> getRecentOrders(GetOrdersRequest req) {
        Map<String, String> param = new HashMap<>();
        param.put("symbol", req.getSymbol());
        param.put("states", req.getStates());
        if (req.getStartDate() != null) {
            param.put("start-date", req.getStartDate());
        }
        if (req.getEndDate() != null) {
            param.put("end-date", req.getEndDate());
        }
        if (req.getStates() != null) {
            param.put("states", req.getStates());
        }
        if (req.getFrom() != null) {
            param.put("from", req.getFrom());
        }
        if (req.getDirect() != null) {
            param.put("direct", req.getDirect());
        }
        if (req.getSize() != null) {
            param.put("size", req.getSize());
        }
        return get("/v1/order/orders", param, new TypeReference<ApiResponse<List<OrderDetail>>>() {});
    }

    public IntrustDetailResponse intrustOrdersDetail(IntrustOrdersDetailRequest req) {
        Map<String, String> map = new HashMap<>();
        map.put("symbol", req.symbol);
        map.put("states", req.states);
        if (req.startDate!=null) {
            map.put("startDate",req.startDate);
        }
        if (req.startDate!=null) {
            map.put("start-date",req.startDate);
        }
        if (req.endDate!=null) {
            map.put("end-date",req.endDate);
        }
        if (req.types!=null) {
            map.put("types",req.types);
        }
        if (req.from!=null) {
            map.put("from",req.from);
        }
        if (req.direct!=null) {
            map.put("direct",req.direct);
        }
        if (req.size!=null) {
            map.put("size",req.size);
        }

        return get("/v1/order/orders/", map, new TypeReference<IntrustDetailResponse<List<IntrustDetail>>>() {});
    }

//  public IntrustDetailResponse getALlOrdersDetail(String orderId) {
//    IntrustDetailResponse resp = get("/v1/order/orders/"+orderId, null,new TypeReference<IntrustDetailResponse>() {});
//    return resp;
//  }


    // send a GET request.
    private <T> T get(String uri, Map<String, String> params, TypeReference<T> ref) {
        if (params == null) {
            params = new HashMap<>();
        }
        return call("GET", uri, null, params, ref);
    }

    // send a POST request.
    private <T> T post(String uri, Object object, TypeReference<T> ref) {
        return call(POST, uri, object, new HashMap<>(), ref);
    }

    // call api by endpoint.
    private <T> T call(String method, String uri, Object object, Map<String, String> params,
                       TypeReference<T> ref) {
        ApiSignature sign = new ApiSignature();
        sign.createSignature(this.accessKeyId, this.accessKeySecret, method, API_HOST, uri, params);
        try {
            Request.Builder builder;
            if ("POST".equals(method)) {
                RequestBody body = RequestBody.create(JSON, JsonUtil.writeValue(object));
                builder = new Request.Builder().url(API_URL + uri + "?" + toQueryString(params)).post(body);
            } else {
                builder = new Request.Builder().url(API_URL + uri + "?" + toQueryString(params)).get();
            }
            if (this.assetPassword != null) {
                builder.addHeader("AuthData", authData());
            }
            Request request = builder.build();
            Response response = client.newCall(request).execute();
            String s = response.body().string();
            return JsonUtil.readValue(s, ref);
        } catch (IOException e) {
            throw new ApiException(e);
        }
    }

    String authData() {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        md.update(this.assetPassword.getBytes(StandardCharsets.UTF_8));
        md.update("hello, moto".getBytes(StandardCharsets.UTF_8));
        Map<String, String> map = new HashMap<>();
        map.put("assetPwd", DatatypeConverter.printHexBinary(md.digest()).toLowerCase());
        try {
            return ApiSignature.urlEncode(JsonUtil.writeValue(map));
        } catch (IOException e) {
            throw new RuntimeException("Get json failed: " + e.getMessage());
        }
    }

    // Encode as "a=1&b=%20&c=&d=AAA"
    String toQueryString(Map<String, String> params) {
        List<String> collect = params.entrySet()
                .stream()
                .map((entry) -> entry.getKey() + "=" + ApiSignature.urlEncode(entry.getValue()))
                .collect(Collectors.toList());
        return String.join("&", collect);
    }

    // create OkHttpClient:
    private static OkHttpClient createOkHttpClient() {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        String httpProxy = System.getenv("http_proxy");
        if (!Strings.isNullOrEmpty(httpProxy)) {
            Proxy proxy = Utils.getProxyFromEnv(httpProxy);
            builder = builder.proxy(proxy);
        }
        return builder.connectTimeout(CONN_TIMEOUT, TimeUnit.SECONDS)
                .readTimeout(READ_TIMEOUT, TimeUnit.SECONDS).writeTimeout(WRITE_TIMEOUT, TimeUnit.SECONDS)
                .build();
    }

    static class ApiSignature {

        static final DateTimeFormatter DT_FORMAT = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss");
        static final ZoneId ZONE_GMT = ZoneId.of("Z");

        /**
         * 创建一个有效的签名。该方法为客户端调用，将在传入的params中添加AccessKeyId、Timestamp、SignatureVersion、SignatureMethod、Signature参数。
         *
         * @param appKey       AppKeyId.
         * @param appSecretKey AppKeySecret.
         * @param method       请求方法，"GET"或"POST"
         * @param host         请求域名，例如"be.huobi.com"
         * @param uri          请求路径，注意不含?以及后的参数，例如"/v1/api/info"
         * @param params       原始请求参数，以Key-Value存储，注意Value不要编码
         */
        void createSignature(String appKey, String appSecretKey, String method, String host,
                             String uri, Map<String, String> params) {
            StringBuilder sb = new StringBuilder(1024);
            sb.append(method.toUpperCase()).append('\n') // GET
                    .append(host.toLowerCase()).append('\n') // Host
                    .append(uri).append('\n'); // /path
            params.remove("Signature");
            params.put("AccessKeyId", appKey);
            params.put("SignatureVersion", "2");
            params.put("SignatureMethod", HMAC_SHA_256);
            params.put("Timestamp", gmtNow());
            // build signature:
            SortedMap<String, String> map = new TreeMap<>(params);
            for (Map.Entry<String, String> entry : map.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                sb.append(key).append('=').append(urlEncode(value)).append('&');
            }
            // remove last '&':
            sb.deleteCharAt(sb.length() - 1);
            // sign:
            Mac hmacSha256;
            try {
                hmacSha256 = Mac.getInstance(HMAC_SHA_256);
                SecretKeySpec secKey =
                        new SecretKeySpec(appSecretKey.getBytes(StandardCharsets.UTF_8), HMAC_SHA_256);
                hmacSha256.init(secKey);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("No such algorithm: " + e.getMessage());
            } catch (InvalidKeyException e) {
                throw new RuntimeException("Invalid key: " + e.getMessage());
            }
            String payload = sb.toString();
            byte[] hash = hmacSha256.doFinal(payload.getBytes(StandardCharsets.UTF_8));
            String actualSign = Base64.getEncoder().encodeToString(hash);
            params.put("Signature", actualSign);


            if (log.isDebugEnabled()) {
                log.debug("Dump parameters:");
                for (Map.Entry<String, String> entry : params.entrySet()) {
                    log.debug("  key: " + entry.getKey() + ", value: " + entry.getValue());
                }
            }
        }


        /**
         * 使用标准URL Encode编码。注意和JDK默认的不同，空格被编码为%20而不是+。
         *
         * @param s String字符串
         * @return URL编码后的字符串
         */
        static String urlEncode(String s) {
            try {
                return URLEncoder.encode(s, "UTF-8").replaceAll("\\+", "%20");
            } catch (UnsupportedEncodingException e) {
                throw new IllegalArgumentException("UTF-8 encoding not supported!");
            }
        }

        /**
         * Return epoch seconds
         */
        long epochNow() {
            return Instant.now().getEpochSecond();
        }

        String gmtNow() {
            return Instant.ofEpochSecond(epochNow()).atZone(ZONE_GMT).format(DT_FORMAT);
        }
    }

    static class JsonUtil {

        static String writeValue(Object obj) throws IOException {
            return objectMapper.writeValueAsString(obj);
        }

        static <T> T readValue(String s, TypeReference<T> ref) throws IOException {
            return objectMapper.readValue(s, ref);
        }

        static final ObjectMapper objectMapper = createObjectMapper();

        static ObjectMapper createObjectMapper() {
            final ObjectMapper mapper = new ObjectMapper();
            mapper.setPropertyNamingStrategy(PropertyNamingStrategy.KEBAB_CASE);
            mapper.setSerializationInclusion(JsonInclude.Include.ALWAYS);
            // disabled features:
            mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
            mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
            mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            return mapper;
        }
    }

}
