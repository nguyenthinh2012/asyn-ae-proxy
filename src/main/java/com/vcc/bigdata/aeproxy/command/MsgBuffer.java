package com.vcc.bigdata.aeproxy.command;

import io.vertx.core.buffer.Buffer;

public class MsgBuffer {
    private Buffer buffer;
    private int length;
    private int index;

    public MsgBuffer() {
        this.buffer = Buffer.buffer();
        this.length = 0;
        this.index = 0;
    }
    public void appendBuffer(Buffer buf){
        this.buffer.appendBuffer(buf);
    }
    public void incIndex(){
        this.index++;
    }
    public Buffer getBuffer() {
        return buffer;
    }

    public void setBuffer(Buffer buffer) {
        this.buffer = buffer;
    }

    public void clear(){
        this.buffer = Buffer.buffer();
        this.length = 0;
        this.index = 0;
    }
    public MsgBuffer clone(){
        MsgBuffer b = new MsgBuffer();
        b.setBuffer(buffer.copy());
        b.setLength(length);
        return b;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }


}
