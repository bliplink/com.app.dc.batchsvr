package com.app.dc.entity;


import com.app.dc.enums.ExceType;

import java.util.ArrayList;
import java.util.List;

public class ExecTablePo<T> {
    public String sql;
    public Object[] args = new Object[]{};
    public List<Object[]> argList = new ArrayList<>();
    public String tableName;
    public Object obj;
    public List<T> insertlist;
    public ExceType type;
    public String[] keys;
}

