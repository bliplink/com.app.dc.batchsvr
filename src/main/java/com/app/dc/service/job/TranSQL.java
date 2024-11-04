package com.app.dc.service.job;

import com.app.common.db.DBUtils;
import com.app.dc.entity.ExecTablePo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;

public class TranSQL {
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    public int tranSql(List<ExecTablePo> poList) {
        int num = -1;
        Connection con = null;
        boolean autoCommit = true;
        try {
            con = DBUtils.getDatabaseConnection().getConnection();
            autoCommit = con.getAutoCommit();
            con.setAutoCommit(false);
            for (ExecTablePo po : poList) {
                switch (po.type) {
                    case insert:
                        DBUtils.insert(po.obj, po.tableName, con);
                        break;
                    case insertList:
                        DBUtils.insertList(po.insertlist, po.tableName, con);
                        break;
                    case delete:
                        DBUtils.update(po.sql, po.args, con);
                        break;
                    case deleteList:
                        DBUtils.updateList(po.sql, po.argList, con);
                        break;
                    case update:
                        DBUtils.updateObject(po.obj, po.tableName, po.keys, con);
                        break;
                }
            }
            con.commit();
            num = 1;
        } catch (Exception e) {
            logger.error("tranSql error:", e);
            if (con != null) {
                try {
                    con.rollback();
                } catch (Exception e1) {
                    logger.error("rollback fail:", e1);
                }
            }

        } finally {
            if (con != null) {
                try {
                    con.setAutoCommit(autoCommit);
                    DBUtils.getDatabaseConnection().freeConnection(con);
                } catch (Exception e1) {
                    logger.error("freeConnection fail:", e1);
                }
            }
        }
        return num;
    }
}
