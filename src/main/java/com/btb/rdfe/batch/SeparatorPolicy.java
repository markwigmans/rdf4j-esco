package com.btb.rdfe.batch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;

@Slf4j
public class SeparatorPolicy extends DefaultRecordSeparatorPolicy {

    private final int fields;

    public SeparatorPolicy(int fields) {
        this.fields = fields;
    }

    @Override
    public boolean isEndOfRecord(String record) {
        if (super.isEndOfRecord(record)) {
            // remove all quoted text, so we remove ',' in these parts as well
            String processed = record.replaceAll("\"([^\"]*)\"", "");
            int length = processed.split(",", -1).length;
            if (length > fields) {
                log.warn("length:{}, vale:'{}'", length, record);
            }
            return length >= fields;
        }
        return false;
    }
}

