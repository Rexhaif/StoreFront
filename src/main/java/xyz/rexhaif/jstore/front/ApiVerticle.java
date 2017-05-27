package xyz.rexhaif.jstore.front;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.LoggerHandler;
import xyz.rexhaif.jstore.front.hashring.ObjectRouter;

import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Rexhaif on 5/25/2017.
 */
public class ApiVerticle extends AbstractVerticle {

    public static final Charset UTF = StandardCharsets.UTF_8;

    private HttpServer mServer;
    private Router mRouter;
    private ObjectRouter mObjectRouter;
    private HttpClient mClient;

    @Override
    public void start(Future<Void> startFuture) throws Exception {

        mObjectRouter = new ObjectRouter(Arrays.asList("52.169.2.146", "bearka.cf"), Consts.REDUNANCY_LEVEL);
        mClient = vertx.createHttpClient();

        mServer = vertx.createHttpServer(
                new HttpServerOptions()
                        .setPort(Consts.PORT)
                        .setTcpKeepAlive(true)
        );

        mRouter = Router.router(vertx);

        mRouter.route().handler(BodyHandler.create());
        mRouter.route().handler(LoggerHandler.create());

        mRouter.post("/:key/").handler(
                rtx -> {
                    byte[] key = rtx.request().getParam("key").getBytes(UTF);
                    byte[] data = rtx.getBody().getBytes();

                    rtx.response().setStatusCode(201);

                    for(String server : mObjectRouter.getServers(key)) {
                        String path = "/" + new String(key, UTF) + "/";
                        mClient.post(8080, server, path)
                                .setChunked(true)
                                .handler(
                                        rsp -> rtx
                                                .response()
                                                .setStatusCode(
                                                        Math.max(
                                                                rtx.response().getStatusCode(),
                                                                rsp.statusCode()
                                                        )
                                                )
                                )
                                .write(Buffer.buffer(data))
                                .end();
                    }

                    rtx.response().end();
                }
        );
        mRouter.get("/:key/").handler(
                rtx -> {
                    byte[] key = rtx.request().getParam("key").getBytes(UTF);

                    AtomicInteger failCounter = new AtomicInteger(0);
                    for (String server : mObjectRouter.getServers(key)) {
                        String path = "/" + new String(key, UTF) + "/";
                        mClient.getNow(8080, server, path, rsp -> {
                            if (rsp.statusCode() == 200) {
                                Buffer buffer = Buffer.buffer();
                                rtx.response().setStatusCode(200).setChunked(true);
                                rsp.bodyHandler(buffer::appendBuffer).endHandler(end -> {
                                    rtx.response().end(buffer);
                                });
                            } else {
                                if (failCounter.incrementAndGet() >= mObjectRouter.getServers(key).size()) {
                                    rtx.response().setStatusCode(404).end();
                                }
                            }
                        });
                    }
                    rtx.response().end();


                }
        );
        mRouter.put("/:key/").handler(
                rtx -> {
                    byte[] key = rtx.request().getParam("key").getBytes(UTF);
                    byte[] data = rtx.getBody().getBytes();
                    rtx.response().setStatusCode(201);

                    for (String server : mObjectRouter.getServers(key)) {
                        String path = "/" + new String(key, UTF) + "/";

                        mClient.put(8080, server, path).setChunked(true).write(Buffer.buffer(data)).handler(
                                rsp -> rtx
                                        .response()
                                        .setStatusCode(
                                                Math.max(
                                                        rtx.response().getStatusCode(),
                                                        rsp.statusCode()
                                                )
                                        )
                        ).end();
                    }
                    rtx.response().end();
                }
        );
        mRouter.delete("/:key/").handler(
                rtx -> {
                    byte[] key = rtx.request().getParam("key").getBytes(UTF);

                    rtx.response().setStatusCode(200);

                    for (String server : mObjectRouter.getServers(key)) {
                        String path = "/" + new String(key, UTF) + "/";

                        mClient.delete(8080, server, path).handler(
                                rsp -> rtx
                                        .response()
                                        .setStatusCode(
                                                Math.max(
                                                        rtx.response().getStatusCode(),
                                                        rsp.statusCode()
                                                )
                                        )
                        ).end();
                    }
                    rtx.response().end();
                }
        );
        mRouter.head("/:key/").handler(
                rtx -> {
                    byte[] key = rtx.request().getParam("key").getBytes(UTF);


                    rtx.response().setStatusCode(200);
                    for (String server : mObjectRouter.getServers(key)) {
                        String path = "/" + new String(key, UTF) + "/";

                        mClient.headNow(
                                8080,
                                server,
                                path,
                                rsp -> rtx
                                        .response()
                                        .setStatusCode(
                                                Math.max(
                                                        rtx.response().getStatusCode(),
                                                        rsp.statusCode()
                                                )
                                        )
                        );
                    }

                    rtx.response().end();
                }
        );

        mServer.requestHandler(mRouter::accept).listen();

        startFuture.complete();
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        mServer.close(result -> {
            if (result.succeeded()) {
                mRouter.clear();
                stopFuture.complete();
            }
        });
    }
}
