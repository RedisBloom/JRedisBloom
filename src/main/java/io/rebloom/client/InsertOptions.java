package io.rebloom.client;

import redis.clients.jedis.Protocol;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static io.rebloom.client.Keywords.*;

public class InsertOptions {
    private final List<byte[]> options = new ArrayList<>();

    /**
     * If specified, should be followed by the desired capacity for the filter to be created
     * @param capacity
     * @return
     */
    public InsertOptions capacity(final long capacity) {
        options.add(CAPACITY.getRaw());
        options.add(Protocol.toByteArray(capacity));
        return this;
    }

    /**
     * If specified, should be followed by the the error ratio of the newly created filter if it does not yet exist
     * @param errorRate
     * @return
     */
    public InsertOptions error(final double errorRate) {
        options.add(ERROR.getRaw());
        options.add(Protocol.toByteArray(errorRate));
        return this;
    }

    /**
     * If specified, indicates that the filter should not be created if it does not already exist
     * It is an error to specify NOCREATE together with either CAPACITY or ERROR .
     * @return
     */
    public InsertOptions nocreate() {
        options.add(NOCREATE.getRaw());
        return this;
    }

    public Collection<byte[]> getOptions() {
        return Collections.unmodifiableCollection(options);
    }
}
