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
import java.util.Arrays;
import java.util.Collections;

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

        mObjectRouter = new ObjectRouter(Collections.singletonList(Consts.localhostNode.toURL()), Consts.REDUNANCY_LEVEL);
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

                    final int[] statusCode = {0};

                    for(URL server : mObjectRouter.getServers(key)) {
                        StringBuilder sb = new StringBuilder(server.toString());
                        sb.append(new String(key, UTF)).append("/");
                        mClient.post(sb.toString())
                                .setChunked(true)
                                .write(Buffer.buffer(data))
                                .handler(rsp -> {
                                    statusCode[0] = Math.max(statusCode[0], rsp.statusCode());
                                })
                                .end();
                    }

                    rtx.response().setStatusCode(statusCode[0]).end();
                }
        );
        mRouter.get("/:key/").handler(
                rtx -> {
                    byte[] key = rtx.request().getParam("key").getBytes(UTF);

                    final boolean[] success = {false};
                    for (URL server : mObjectRouter.getServers(key)) {
                        if (success[0]) break;
                        StringBuilder sb = new StringBuilder(server.toString());
                        sb.append(new String(key, UTF)).append("/");


                        mClient.get(sb.toString()).handler(rsp -> {
                            if (rsp.statusCode() == 200) {
                                success[0] = true;
                                rsp.bodyHandler(buffer ->
                                        rtx.response()
                                                .setStatusCode(200)
                                                .setChunked(true)
                                                .write(buffer)
                                                .end()
                                );
                            }
                        }).end();
                    }
                }
        );
        mRouter.put("/:key/").handler(
                rtx -> {
                    byte[] key = rtx.request().getParam("key").getBytes(UTF);
                    byte[] data = rtx.getBody().getBytes();
                    final int[] statusCode = {0};

                    for (URL server : mObjectRouter.getServers(key)) {
                        StringBuilder sb = new StringBuilder(server.toString());
                        sb.append(new String(key, UTF)).append("/");

                        mClient.put(sb.toString()).setChunked(true).write(Buffer.buffer(data)).handler(
                                rsp -> statusCode[0] = Math.max(statusCode[0], rsp.statusCode())
                        ).end();
                    }
                    rtx.response().setStatusCode(statusCode[0]).end();
                }
        );
        mRouter.delete("/:key/").handler(
                rtx -> {
                    byte[] key = rtx.request().getParam("key").getBytes(UTF);

                    final int[] statusCode = {0};

                    for (URL server : mObjectRouter.getServers(key)) {
                        StringBuilder sb = new StringBuilder(server.toString());
                        sb.append(new String(key, UTF)).append("/");

                        mClient.delete(sb.toString()).handler(
                                rsp -> statusCode[0] = Math.max(statusCode[0], rsp.statusCode())
                        ).end();
                    }
                    rtx.response().setStatusCode(statusCode[0]).end();
                }
        );
        mRouter.head("/:key/").handler(
                rtx -> {
                    byte[] key = rtx.request().getParam("key").getBytes(UTF);

                    final int[] statusCode = {0};

                    for (URL server : mObjectRouter.getServers(key)) {
                        StringBuilder sb = new StringBuilder(server.toString());
                        sb.append(new String(key, UTF)).append("/");

                        mClient.head(sb.toString()).handler(
                                rsp -> statusCode[0] = Math.max(statusCode[0], rsp.statusCode())
                        ).end();
                    }
                    rtx.response().setStatusCode(statusCode[0]).end();
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
