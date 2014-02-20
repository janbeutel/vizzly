/*
 * Copyright 2012 ETH Zurich, Computer Engineering and Networks Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.ethz.vizzly.datatype;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

/**
 * This class defines a data type that represents a server that listens on a TCP port.
 * @author Matthias Keller
 *
 */
public class ServerSpec {
    
    public InetAddress serverIp;
    public int serverPort;
    private String addressString;
    
    /**
     * Log.
     */
    private static Logger log = Logger.getLogger(ServerSpec.class);
    
    public ServerSpec(InetAddress serverIp, int serverPort, String address) {
        this.serverIp = serverIp;
        this.serverPort = serverPort;
        this.addressString = address;
    }
    
    public static ServerSpec fromAddress(String address)  throws VizzlyException {
        InetAddress serverIp = null;
        int serverPort = 80; // standard HTTP port
        try {
            if(address.contains(":")) {
                String[] arr = address.split(":");
                serverIp = InetAddress.getByName(arr[0]);
                serverPort = Integer.parseInt(arr[1]);
            } else {
                serverIp = InetAddress.getByName(address);
            }
            return new ServerSpec(serverIp, serverPort, address);
        } catch(UnknownHostException e) {
            log.error("Host not found: "+address);
            throw new VizzlyException("Unknown host: "+address);
        }
    }
    
    public boolean equals(Object other) {
        if(this == other) return true;
        if(!(other instanceof ServerSpec)) return false;
        
        ServerSpec otherSpec = (ServerSpec)other;
        return serverIp.equals(otherSpec.serverIp) && addressString.compareToIgnoreCase(addressString) == 0 && serverPort == otherSpec.serverPort;
    }
    
    public int hashCode() {
        return serverIp.hashCode()+serverPort*100;
    }
}
