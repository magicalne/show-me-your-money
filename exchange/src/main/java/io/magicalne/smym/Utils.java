package io.magicalne.smym;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.zip.GZIPInputStream;

@Slf4j
public class Utils {

    public static byte[] ungzip(byte[] source) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(source);
        GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        int res = 0;
        byte buf[] = new byte[1024];
        while (res >= 0) {
            res = gzipInputStream.read(buf, 0, buf.length);
            if (res > 0) {
                byteArrayOutputStream.write(buf, 0, res);
            }
        }
        return byteArrayOutputStream.toByteArray();
    }

    public static Proxy getProxyFromEnv(String httpProxy) {
        int start = httpProxy.lastIndexOf("/");
        int end = httpProxy.indexOf(':', start + 1);
        String host = httpProxy.substring(start + 1, end);
        String port = httpProxy.substring(end + 1, httpProxy.length());
        log.info("host: {}, port: {}", host, port);
        return new Proxy(Proxy.Type.HTTP, new InetSocketAddress(host, Integer.parseInt(port)));
    }

}
