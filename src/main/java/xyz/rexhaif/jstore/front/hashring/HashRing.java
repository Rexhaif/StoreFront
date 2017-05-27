package xyz.rexhaif.jstore.front.hashring;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.net.URL;
import java.util.List;

public class HashRing {

    private List<String> mServers;
    private HashFunction mFunction;


    public HashRing(List<String> servers, HashFunction function) {
        mServers = servers;
        mFunction = function;
    }

    public HashFunction getFunction() {
        return mFunction;
    }

    public List<String> getServers() {
        return mServers;
    }

    public String selectServer(byte[] key) {
        return mServers.get(Hashing.consistentHash(mFunction.hashBytes(key), mServers.size()));
    }

}
