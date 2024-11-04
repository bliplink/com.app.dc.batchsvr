package com.app.dc.entity;

import com.app.dc.fix.message.MarketDataSnapshotFullRefresh;

import java.util.List;

public class MarketDataResult {
    public int totalSize;
    public String cid;
    public List<MarketDataSnapshotFullRefresh> data;
}
