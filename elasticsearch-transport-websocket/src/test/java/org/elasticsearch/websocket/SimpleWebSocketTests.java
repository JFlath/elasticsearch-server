/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.websocket;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.elasticsearch.websocket.client.WebSocketActionListener;
import org.elasticsearch.websocket.client.WebSocketClient;
import org.elasticsearch.websocket.client.WebSocketClientFactory;
import org.elasticsearch.websocket.client.WebSocketClientRequest;
import org.elasticsearch.websocket.http.netty.client.NettyWebSocketClientFactory;

import org.jboss.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.WebSocketFrame;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class SimpleWebSocketTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder().put("index.number_of_shards", numberOfShards()).put("index.number_of_replicas", 0).build();
        for (int i = 0; i < numberOfNodes(); i++) {
            startNode("node" + i, settings);
        }
        client = getClient();
    }

    protected int numberOfShards() {
        return 1;
    }

    protected int numberOfNodes() {
        return 1;
    }

    protected int numberOfRuns() {
        return 1;
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node0");
    }
    
    
    @Test
    public void helloWorld() {
        try {
            final WebSocketClientFactory clientFactory = new NettyWebSocketClientFactory();
            WebSocketClient wsclient = clientFactory.newClient(new URI("ws://localhost:9400/websocket"),
                    new WebSocketActionListener() {
                        @Override
                        public void onConnect(WebSocketClient client) {
                            logger.info("web socket connected");
                            client.send(new TextWebSocketFrame("Hello World"));
                        }

                        @Override
                        public void onDisconnect(WebSocketClient client) {
                            logger.info("web socket disconnected");
                        }

                        @Override
                        public void onMessage(WebSocketClient client, WebSocketFrame frame) {
                            logger.info("frame received: " + frame);
                        }

                        @Override
                        public void onError(Throwable t) {
                            logger.error(t.getMessage(), t);
                        }
                    });
            wsclient.connect().await(1000, TimeUnit.MILLISECONDS);
            Thread.sleep(1000);
            wsclient.send(new CloseWebSocketFrame());
            Thread.sleep(1000);
            wsclient.disconnect();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Test
    public void testBulk() {
        try {
            final WebSocketClientFactory clientFactory = new NettyWebSocketClientFactory();
            WebSocketClient wsclient = clientFactory.newClient(new URI("ws://localhost:9400/websocket"),
                    new WebSocketActionListener.Adapter() {
                        @Override
                        public void onConnect(WebSocketClient client) {
                            try {
                                logger.info("sending some index requests (longer than a bulk size)");
                                for (int i = 0; i < 250; i++) {
                                    clientFactory.indexRequest()
                                            .data(jsonBuilder()                                            
                                            .startObject()
                                            .field("index", "test")
                                            .field("type", "test")
                                            .field("id", Integer.toString(i))
                                            .startObject("data")
                                            .field("field1", "value" + i)
                                            .field("field2", "value" + i)
                                            .endObject()
                                            .endObject())
                                            .send(client);
                                }
                                // more bulks could be added here ...
                                logger.info("at the end, let us flush the bulk");
                                clientFactory.flushRequest().send(client);
                            } catch (Exception e) {
                                onError(e);
                            }
                        }

                        @Override
                        public void onDisconnect(WebSocketClient client) {
                            logger.info("disconnected");
                        }

                        @Override
                        public void onMessage(WebSocketClient client, WebSocketFrame frame) {
                            logger.info("frame received: {}", frame);
                        }

                        @Override
                        public void onError(Throwable t) {
                            logger.error(t.getMessage(), t);
                        }
                    });
            wsclient.connect().await(1000, TimeUnit.MILLISECONDS);
            Thread.sleep(1000);
            logger.info("closing bulk client");
            wsclient.send(new CloseWebSocketFrame());
            Thread.sleep(1000);
            wsclient.disconnect();
            clientFactory.shutdown();
            
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

    }


    /**
     * In this test, we just use simple TextWebSocketFrame to send publish/subscribe requests.
     */
    @Test
    public void subscribeToOurselves() {
        try {
            final WebSocketClientFactory clientFactory = new NettyWebSocketClientFactory();
            WebSocketClient wsclient = clientFactory.newClient(new URI("ws://localhost:9400/websocket"),
                    new WebSocketActionListener() {
                        @Override
                        public void onConnect(WebSocketClient client) {
                            try {
                                logger.info("sending subscribe command");
                                client.send(new TextWebSocketFrame("{\"type\":\"subscribe\",\"data\":{\"subscriber\":\"mypubsubdemo\",\"topic\":\"demo\"}}"));
                                Thread.sleep(500);
                                logger.info("sending publish command (to ourselves)");
                                client.send(new TextWebSocketFrame("{\"type\":\"publish\",\"data\":{\"message\":\"Hello World\",\"topic\":\"demo\"}}"));
                            } catch (Exception e) {
                                logger.error(e.getMessage(), e);
                            }
                        }

                        @Override
                        public void onDisconnect(WebSocketClient client) {
                            logger.info("web socket disconnected");
                        }

                        @Override
                        public void onMessage(WebSocketClient client, WebSocketFrame frame) {
                            logger.info("frame received: {}", frame);
                        }

                        @Override
                        public void onError(Throwable t) {
                            logger.error(t.getMessage(), t);
                        }
                    });
            wsclient.connect().await(1000, TimeUnit.MILLISECONDS);            
            Thread.sleep(1000);
            wsclient.send(new CloseWebSocketFrame());
            Thread.sleep(1000);
            wsclient.disconnect();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
    
    public void testOneClient() {
        try {
            final String subscriberId = "oneclient";
            final String topic = "oneclienttest";
            final URI uri = new URI("ws://localhost:9400/websocket");
            final WebSocketClientFactory clientFactory = new NettyWebSocketClientFactory();
            
            final WebSocketClientRequest subscribe = clientFactory.newRequest()
                    .type("subscribe").data(jsonBuilder().startObject()
                    .field("subscriber", "singleclient")
                    .field("topic", topic).endObject());
            
            final WebSocketClientRequest publish = clientFactory.newRequest()
                    .type("publish").data(jsonBuilder().startObject()
                    .field("message", "Hello World")
                    .field("topic", topic).endObject());
            
            WebSocketClient client = clientFactory.newClient(uri,
                    new WebSocketActionListener.Adapter() {
                        @Override
                        public void onConnect(WebSocketClient client) throws IOException {
                                logger.info("sending subscribe command, channel = {}", client.channel());
                                subscribe.send(client);
                                logger.info("sending publish command (to ourselves), channel = {}", client.channel());
                                publish.send(client);
                        }

                        @Override
                        public void onMessage(WebSocketClient client, WebSocketFrame frame) {
                            logger.info("frame received: " + frame);
                        }
                    });
            client.connect().await(1000, TimeUnit.MILLISECONDS);            
            Thread.sleep(1000);
            client.send(new CloseWebSocketFrame());
            Thread.sleep(1000);
            client.disconnect();
            
            clientFactory.shutdown();
            
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }        
    }

    @Test
    public void testTwoClients() {
        try {
            final String subscriberId = "twoclients";
            final String topic = "twoclienttest";
            final URI uri = new URI("ws://localhost:9400/websocket");
            final WebSocketClientFactory clientFactory = new NettyWebSocketClientFactory();
            final WebSocketClientRequest subscribe = clientFactory.newRequest()
                    .type("subscribe").data(jsonBuilder().startObject()
                    .field("subscriber", "doubleclient")
                    .field("topic", topic).endObject());
            
            final WebSocketClientRequest publish = clientFactory.newRequest()
                    .type("publish").data(jsonBuilder().startObject()
                    .field("message", "Hi there, I'm another client")
                    .field("topic", topic).endObject());
            
            // open first client
            WebSocketClient subscribingClient = clientFactory.newClient(uri,
                    new WebSocketActionListener.Adapter() {
                        @Override
                        public void onConnect(WebSocketClient client) {
                            try {
                                logger.info("sending subscribe command, channel {}", client.channel());
                                subscribe.send(client);
                            } catch (Exception e) {
                            }
                        }

                        @Override
                        public void onMessage(WebSocketClient client, WebSocketFrame frame) {
                            logger.info("frame {} received for subscribing client {}", frame, client.channel());
                        }

                    });
            
            // open two client
            WebSocketClient publishingClient = clientFactory.newClient(uri,
                    new WebSocketActionListener.Adapter() {
                        @Override
                        public void onConnect(WebSocketClient client) {
                            try {
                                logger.info("sending publish command, channel = {}", client.channel());
                                publish.send(client);
                            } catch (Exception e) {
                            }
                        }

                        @Override
                        public void onMessage(WebSocketClient client, WebSocketFrame frame) {
                            logger.info("frame {} received for publishing client {}", frame, client.channel());
                        }

                    });
            
            // connect both clients to node
            subscribingClient.connect().await(1000, TimeUnit.MILLISECONDS);            
            publishingClient.connect().await(1000, TimeUnit.MILLISECONDS);            
            
            // wait for publish/subscribe
            Thread.sleep(1000);
            
            // close first client
            publishingClient.send(new CloseWebSocketFrame());
            publishingClient.disconnect();

            // close second client
            subscribingClient.send(new CloseWebSocketFrame());
            subscribingClient.disconnect();
            
            Thread.sleep(1000);
            
            clientFactory.shutdown();
            
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }        
    }
    
}