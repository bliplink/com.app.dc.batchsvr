package com.app.dc.entity;

import lombok.Data;

import java.util.List;

@Data
public class LPValueResult {
    public List<LPValue> data;
    public int code;

    public String msg;
}
