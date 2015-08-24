/*
* Copyright (c) 2015 Uber Technologies, Inc.
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included in
* all copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
* THE SOFTWARE.
*/

package com.uber.tchannel.hyperbahn;


import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.hyperbahn.messages.AdvertiseRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class HyperbahnClient {

    private static final int ADVERTISE_INTERVAL = 50 * 1000;
    private static final int ADVERTISE_FUZZ_INTERVAL = 20 * 1000;
    private static final int ADVERTISE_RETRY_INTERVAL = 1 * 1000;
    private static final String HYPERBAHN_SERVICE_NAME = "hyperbahn";

    private final List<TChannel> channels;
    private final Random random;

    public static class Builder {
        private final List<TChannel> channelList;

        public Builder() {
            channelList = new ArrayList<TChannel>();
        }

        public Builder addChannel(TChannel channel) {
            channelList.add(channel);
            return this;
        }

        public HyperbahnClient build() {
            return new HyperbahnClient(this);
        }

    }

    private HyperbahnClient(Builder builder) {
        channels = builder.channelList;
        random = new Random();
    }

    private int generateFuzzedAdvertiseInternval() {
        return ADVERTISE_INTERVAL + random.nextInt(ADVERTISE_FUZZ_INTERVAL);
    }

    private AdvertiseRequest buildRequest() {
        AdvertiseRequest request = new AdvertiseRequest();
        for (TChannel channel: channels) {
            request.addService(channel.getService(), 0);
        }
        return request;
    }

    public void advertise() {
        Thread daemon = new Thread(new Runnable() {
            @Override
            public void run() {

                while(true) {
                    int sleepFor = generateFuzzedAdvertiseInternval();
                    try {
                        Thread.sleep(sleepFor);
                    } catch (InterruptedException exception) {

                    }


                }

            }
        });
        daemon.setDaemon(true);
        daemon.start();
    }
}
