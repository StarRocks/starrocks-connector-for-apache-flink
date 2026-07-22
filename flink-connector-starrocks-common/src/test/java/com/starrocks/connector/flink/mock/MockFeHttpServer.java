/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.flink.mock;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;

/** Mock Fe http server. */
public class MockFeHttpServer implements Closeable {

    public static final String NULL_RESPONSE = "";

    private final int expectedListenPort;

    private ServerSocket serverSocket;
    private int listenPort;
    private Thread listenThread;
    private final ConcurrentHashMap<Long, Thread> runningAcceptThread = new ConcurrentHashMap<>();
    private final Queue<String> pendingResponse = new ConcurrentLinkedQueue<>();
    private volatile boolean closed;

    public MockFeHttpServer() {
        this(-1);
    }

    public MockFeHttpServer(int expectedListenPort) {
        this.expectedListenPort = expectedListenPort;
    }

    public void start() throws Exception {
        if (expectedListenPort >= 0) {
            listenPort = expectedListenPort;
            serverSocket = new ServerSocket(listenPort);
        } else {
            int numRetries = 0;
            while (true) {
                numRetries += 1;
                try {
                    listenPort = ThreadLocalRandom.current().nextInt(10000, 65536);
                    serverSocket = new ServerSocket(listenPort);
                    break;
                } catch (IOException e) {
                    if (numRetries >= 100) {
                        throw e;
                    }
                }
            }
        }

        this.listenThread = new Thread(new ListenRunnable(serverSocket));
        listenThread.start();
    }

    public void addJsonResponse(String jsonResponse) {
        this.pendingResponse.add(jsonResponse);
    }

    public int getListenPort() {
        return listenPort;
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
        if (serverSocket != null) {
            serverSocket.close();
        }

        for (Thread thread : runningAcceptThread.values()) {
            thread.interrupt();
            try {
                thread.join();
            } catch (Exception e) {
                // ignore
            }
        }

        if (listenThread != null) {
            listenThread.interrupt();
            try {
                listenThread.join();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    private class ListenRunnable implements Runnable {

        private final ServerSocket serverSocket;

        public ListenRunnable(ServerSocket serverSocket) {
            this.serverSocket = serverSocket;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    AcceptRunnable acceptRunnable = new AcceptRunnable(socket, pendingResponse.poll());
                    Thread thread = new Thread(acceptRunnable);
                    runningAcceptThread.put(thread.getId(), thread);
                    thread.start();
                } catch (Exception e) {
                    if (MockFeHttpServer.this.closed) {
                        break;
                    }
                }
            }
        }
    }

    private class AcceptRunnable implements Runnable {

        private final Socket socket;
        private final String jsonResponse;

        public AcceptRunnable(Socket socket, String jsonResponse) {
            this.socket = socket;
            this.jsonResponse = jsonResponse;
        }

        @Override
        public void run() {
            try {
                if (jsonResponse == NULL_RESPONSE) {
                    return;
                }
                InputStream in = socket.getInputStream();
                OutputStream out = socket.getOutputStream();
                String body = jsonResponse;
                if (body.length() > 0) {
                    JSONObject tmp = JSON.parseObject(body);
                    if (tmp.containsKey("LoadTimeMs")) {
                        Thread.sleep(tmp.getLong("LoadTimeMs"));
                    }
                }
                String res = "HTTP/1.1 200 OK\r\n" +
                        "Content-Length:" + body.length() + "\r\n" +
                        "Connection:close\r\n" +
                        "\r\n" + body;
                out.write(res.getBytes());
                out.flush();
                out.close();
                in.close();
            } catch (Exception e) {
                // ignore
            } finally {
                try {
                    socket.close();
                } catch (Exception e) {
                    //ignore
                }
                MockFeHttpServer.this.runningAcceptThread.remove(Thread.currentThread().getId());
            }
        }
    }
}
