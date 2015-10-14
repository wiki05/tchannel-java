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

package com.uber.tchannel.benchmarks;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.schemes.RawRequest;
import com.uber.tchannel.schemes.RawResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RawClient {

    private final String host;
    private final int port;

    public RawClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("h", "host", true, "Server Host to connect to");
        options.addOption("p", "port", true, "Server Port to connect to");
        options.addOption("n", "requests", true, "Number of requests to make");
        options.addOption("?", "help", false, "Usage");
        HelpFormatter formatter = new HelpFormatter();

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("?")) {
            formatter.printHelp("PingClient", options, true);
            return;
        }

        String host = cmd.getOptionValue("h", "0.0.0.0");
        int port = Integer.parseInt(cmd.getOptionValue("p", "8888"));

        System.out.println(String.format("Connecting from client to server %s on port: %d", host, port));
        new RawClient(host, port).run();
        System.out.println("Stopping Client...");

    }

    public void run() throws Exception {

        Long endTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);

        int actualQueryCount = makeCalls(ByteBufAllocator.DEFAULT.buffer(1024), endTime);
        System.out.println("The QPS for this iteration of the benchmark was " + actualQueryCount / 60.0);
    }

    public int makeCalls(final ByteBuf buffer, final long endTime) throws Exception {

        final AtomicInteger roundTripQueryCount = new AtomicInteger();
        final AtomicInteger clientQueryCount = new AtomicInteger();

        TChannel client = new TChannel.Builder("raw-client").build();

        while (System.nanoTime() < endTime) {

            ByteBuf headers = ByteBufAllocator.DEFAULT.buffer();

            RawRequest request = new RawRequest(
                    1000,
                    "raw",
                    new HashMap<String, String>(),
                    Unpooled.wrappedBuffer("raw".getBytes()),
                    headers,
                    buffer.copy()
            );

            ListenableFuture<RawResponse> future = client.call(
                    InetAddress.getByName(host),
                    port,
                    request
            );

            clientQueryCount.getAndIncrement();

            Futures.addCallback(future, new FutureCallback<RawResponse>() {
                @Override
                public void onSuccess(RawResponse response) {

                    if (System.nanoTime() < endTime) {
                        roundTripQueryCount.incrementAndGet();
                    }

                    response.getArg1().release();
                    response.getArg2().release();
                    response.getArg3().release();
                }

                @Override
                public void onFailure(Throwable err) {
                    System.out.println("Failure");
                }
            });
        }

        Thread.sleep(5000);
        System.out.println("The client made " + clientQueryCount.get() + " calls");
        return roundTripQueryCount.get();

    }
}

