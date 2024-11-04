package com.app.dc.enums;

public enum ToChainStatus {
    Success("1"),Fail("2"),DidNot("0");

    private String value;

    ToChainStatus(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
