package com.pinterest.secor.parser;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;

import java.util.Date;
import java.text.SimpleDateFormat;

/**
 *
 * @author Ali Rakhshanfar
 */
public class PGMessageParser extends MessageParser {
    private String [] result;
    public PGMessageParser(SecorConfig config) {
        super(config);
        result = new String[1];
        result[0] = new SimpleDateFormat("yyyy/M/d/H").format(new Date());
    }

    @Override
    public String[] extractPartitions(Message message) throws Exception {
        return result;
    }
}
