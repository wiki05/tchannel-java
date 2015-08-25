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

import com.uber.tchannel.api.Request;
import com.uber.tchannel.api.Response;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.hyperbahn.messages.AdvertiseRequest;
import com.uber.tchannel.hyperbahn.messages.AdvertiseResponse;
import io.netty.util.concurrent.Promise;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class HyperbahnClient {

    private static final int ADVERTISE_INTERVAL = 50 * 1000;
    private static final int ADVERTISE_FUZZ_INTERVAL = 20 * 1000;
    private static final int ADVERTISE_RETRY_INTERVAL = 1 * 1000;
    private static final int RESPONSE_WAIT = 1 * 1000;

    private static final String HYPERBAHN_SERVICE_NAME = "hyperbahn";

    private final TChannel channel;
    private final List<InetAddress> hosts;
    private final List<Integer> ports;
    private final Random random;

    public static class Builder {
        private final List<InetAddress> hosts;
        private final List<Integer> ports;
        private final TChannel channel;

        public Builder(TChannel channel) {
            this.channel = channel;
            hosts = new ArrayList<InetAddress>();
            ports = new ArrayList<Integer>();
        }

        public Builder addPeer(String host, int port) throws UnknownHostException {
            hosts.add(InetAddress.getByName(host));
            ports.add(port);
            return this;
        }

        // TODO add configuration
        public HyperbahnClient build() throws UnknownHostException {

            if (this.hosts.isEmpty()) {
                throw new IllegalArgumentException("Hyperbahn hosts are missing");
            }
            return new HyperbahnClient(this);
        }

    }

    private HyperbahnClient(Builder builder) throws UnknownHostException {

        this.hosts = builder.hosts;
        this.ports = builder.ports;
        this.channel = builder.channel;
        this.random = new Random();
    }

    private int fuzzInternal(int interval) {
        return random.nextInt(interval);
    }

    private int generateFuzzedAdvertiseInternval() {
        return ADVERTISE_INTERVAL + fuzzInternal(ADVERTISE_FUZZ_INTERVAL);
    }

    private AdvertiseRequest buildRequest() {
        AdvertiseRequest request = new AdvertiseRequest();
        request.addService(channel.getService(), 0);
        return request;

    }

    private void advertisePerPeer(final TChannel channel, final InetAddress host, final int port) {
        Thread daemon = new Thread(new Runnable() {
            @Override
            public void run() {

                int consecutiveFailures = 0;
                // TODO disable tracing for these requests
                AdvertiseRequest advertiseRequest = buildRequest();

                Request<AdvertiseRequest> request = new Request.Builder<AdvertiseRequest>(advertiseRequest)
                        .setService(HYPERBAHN_SERVICE_NAME)
                        .build();

                while(true) {
                    int sleepFor;
                    try {
                        Promise<Response<AdvertiseResponse>> promise = channel.callJSON(
                                host,
                                port,
                                request,
                                AdvertiseResponse.class

                        );

                        promise.await(RESPONSE_WAIT);

                        if(promise.isSuccess()) {
                            // TODO add logging
                            consecutiveFailures = 0;
                            sleepFor = generateFuzzedAdvertiseInternval();
                        }
                        else {
                            sleepFor = fuzzInternal(ADVERTISE_RETRY_INTERVAL * (1 << consecutiveFailures));
                            consecutiveFailures += 1;
                        }

                    } catch (InterruptedException exception) {
                        // TODO try avoiding this repetition
                        sleepFor = fuzzInternal(ADVERTISE_RETRY_INTERVAL * (1 << consecutiveFailures));
                        consecutiveFailures += 1;
                    }

                    // TODO if a lot of consecutive calls fails then notify end developer
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
    public void advertise() {
        // Lets start a daemonthread per peer for advertising
        for (int i=0; i < hosts.size(); i++) {
            advertisePerPeer(channel, hosts.get(i), ports.get(i));
        }
    }
}
