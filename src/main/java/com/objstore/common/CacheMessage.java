package com.objstore.common;

import java.io.Serializable;

public class CacheMessage implements Serializable {
    public String operation;
    public String key;
    public Serializable value;
    public String replyAddress;
}