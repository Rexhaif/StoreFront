package xyz.rexhaif.jstore.front;

import io.vertx.core.Vertx;
import java.net.MalformedURLException;
/**
 * Created by Rexhaif on 5/23/2017.
 */
public class Launcher {

    public static void main(String[] args) throws MalformedURLException {
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new ApiVerticle());
    }

}
