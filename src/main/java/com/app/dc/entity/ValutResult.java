package com.app.dc.entity;

import java.util.List;

import com.app.dc.po.SolanaTokenBalance;

import lombok.Data;
@Data
public class ValutResult {
	public List<SolanaTokenBalance> data;
    public int code;

    public String msg;
}
