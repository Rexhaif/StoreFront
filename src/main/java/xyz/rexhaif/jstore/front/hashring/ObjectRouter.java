package xyz.rexhaif.jstore.front.hashring;

import com.google.common.hash.HashFunction;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class ObjectRouter {

    private List<HashRing> hashRings;

    public ObjectRouter(List<String> servers, int backups) {

        hashRings = new CopyOnWriteArrayList<>();

        int numOfBackups = backups + 1;
        if (backups > HashFunctionSet.maximumNumberOfFunctions()) {
            System.err.println("backups number are higher than number of available hash functions, trimming it to applicable size");
            numOfBackups = backups % HashFunctionSet.maximumNumberOfFunctions();
        }

        List<HashFunction> functions = HashFunctionSet.getUniqueHashes(numOfBackups);

        functions.forEach(func -> hashRings.add(new HashRing(servers, func)));
    }

    public JsonObject toJsonObject() {
        JsonObject object = new JsonObject();
        JsonArray array = new JsonArray();
        hashRings.forEach(ring -> {
            JsonObject ringObj = new JsonObject();
            ringObj.put("servers", new JsonArray(ring.getServers()));
            ringObj.put("function", ring.getFunction().toString());
            array.add(ringObj);
        });
        object.put("hashRing", array);
        return object;
    }

    public List<String> getServers(byte[] key) {
        List<String> servers = new ArrayList<>();
        hashRings.forEach(ring -> servers.add(ring.selectServer(key)));
        return servers;
    }

}
