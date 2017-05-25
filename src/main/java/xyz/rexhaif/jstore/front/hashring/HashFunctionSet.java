package xyz.rexhaif.jstore.front.hashring;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Rexhaif on 5/25/2017.
 */
public class HashFunctionSet {

    private static Map<String, HashFunction> functions = new ConcurrentHashMap<>();

    static {
        functions.put(Hashing.adler32().toString(), Hashing.adler32());
        functions.put(Hashing.crc32c().toString(), Hashing.crc32c());
        functions.put(Hashing.md5().toString(), Hashing.md5());
        functions.put(Hashing.murmur3_32().toString(), Hashing.murmur3_32());
        functions.put(Hashing.sha256().toString(), Hashing.sha256());
        functions.put(Hashing.murmur3_128().toString(), Hashing.murmur3_128());
        functions.put(Hashing.sipHash24().toString(), Hashing.sipHash24());
    }

    public static int maximumNumberOfFunctions() {
        return functions.size();
    }

    public static HashFunction forName(String name) {
        return functions.get(name);
    }

    public static List<HashFunction> getUniqueHashes(int numOfHashes) {
        if (numOfHashes > functions.size()) {
            throw new IllegalArgumentException("numOfHashes must be below or equal than " + functions.size());
        }
        List<HashFunction> functionsList = new ArrayList<>();
        int num = 1;
        for (HashFunction func : functions.values()) {
            functionsList.add(func);
            num++;
            if (num > numOfHashes) {
                break;
            }
        }
        return functionsList;
    }

}
