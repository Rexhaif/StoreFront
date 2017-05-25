package xyz.rexhaif.jstore.front.hashring;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.net.URL;
import java.util.List;

public class HashRing {

    private List<URL> mServers;
    private HashFunction mFunction;


    public HashRing(List<URL> servers, HashFunction function) {
        mServers = servers;
        mFunction = function;
    }

    public HashFunction getFunction() {
        return mFunction;
    }

    public List<URL> getServers() {
        return mServers;
    }

    public URL selectServer(byte[] key) {
        return mServers.get(Hashing.consistentHash(mFunction.hashBytes(key), mServers.size() - 1));
    }

}
