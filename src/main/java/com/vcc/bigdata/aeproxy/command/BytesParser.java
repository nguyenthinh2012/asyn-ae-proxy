package com.vcc.bigdata.aeproxy.command;

import io.vertx.core.buffer.Buffer;

public class BytesParser {
    public static String parse(Buffer buf) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < buf.length(); i++) {
            sb.append(buf.getByte(i) + "|");
        }
        return sb.toString();
    }
}
