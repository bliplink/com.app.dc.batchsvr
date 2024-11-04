package com.app.dc.util;

import com.app.dc.engine.po.Position;
import com.app.dc.engine.po.PositionStatus;
import com.app.dc.engine.po.PositionType;
import com.app.dc.entity.TOrdersPositionDay;
import com.app.dc.po.TOrdersPosition;

public class ObjectConvert {

    public static Position ordersPositionToPosition(TOrdersPositionDay ordersPosition) {
        Position position = new Position();
        position.setUserID(ordersPosition.user_id);
        position.setAccountID(ordersPosition.account_id);
        position.setSymbol(ordersPosition.symbol);
        position.setSecurityID(ordersPosition.security_id);
        position.setCurrency(ordersPosition.currency);
        position.setPositionStatus(PositionStatus.Normal);
        position.setPositionType(ordersPosition.position_type.equals("0") ? PositionType.Cross : PositionType.Isolated);
        position.setLeverage(ordersPosition.leverage);
        position.setLongPosition(ordersPosition.long_position);
        position.setLongAverage(ordersPosition.long_average);
        position.setLongUsedMargin(ordersPosition.long_used_margin);
        position.setShortPosition(ordersPosition.short_position);
        position.setShortAverage(ordersPosition.short_average);
        position.setShortUsedMargin(ordersPosition.short_used_margin);
        position.setLongLockedPosition(ordersPosition.long_locked_position);
        position.setShortLockedPosition(ordersPosition.short_locked_position);
        position.setLongLiqPrice(ordersPosition.long_liq_price);
        position.setShortLiqPrice(ordersPosition.short_liq_price);
        position.setLocation(ordersPosition.location);
        position.setInfo1(ordersPosition.inf1);
        position.setInfo2(ordersPosition.inf2);
        position.setInfo3(ordersPosition.inf3);
        position.setInfo4(ordersPosition.inf4);
        position.setInfo5(ordersPosition.inf5);
        position.setMarketIndicator(ordersPosition.market_indicator);
        return position;
    }
}
