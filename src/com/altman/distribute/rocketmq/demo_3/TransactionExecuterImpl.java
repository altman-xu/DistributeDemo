package com.altman.distribute.rocketmq.demo_3;

import com.alibaba.rocketmq.client.producer.LocalTransactionExecuter;
import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.common.message.Message;

/**
 * @author xuzhihua
 * @date 2019/2/13 11:10 PM
 */
public class TransactionExecuterImpl implements LocalTransactionExecuter {
    @Override
    public LocalTransactionState executeLocalTransactionBranch(Message msg, Object arg) {
        System.out.println("msg = " + new String(msg.getBody()));
        System.out.println("arg = " + arg);
        String tag = msg.getTags();
        if (tag.equals("Transaction3")) {
            // 这里有一个分阶段提交任务的概念
            System.out.println("这里处理业务逻辑，比如操作数据库，失败情况下进行ROLLBACK");
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        return LocalTransactionState.COMMIT_MESSAGE;
//        return LocalTransactionState.ROLLBACK_MESSAGE;
//        return LocalTransactionState.UNKNOW;
    }
}
