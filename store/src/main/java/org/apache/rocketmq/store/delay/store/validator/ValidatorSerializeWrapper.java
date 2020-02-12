package org.apache.rocketmq.store.delay.store.validator;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashMap;
import java.util.Map;

public class ValidatorSerializeWrapper extends RemotingSerializable {

    private Map<Long, Map<String, Long>> offsetTable =  new HashMap<>();

    public Map<Long, Map<String, Long>> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(Map<Long, Map<String, Long>> offsetTable) {
        this.offsetTable = offsetTable;
    }
}
