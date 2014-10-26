/*
 * Copyright 2014 Alexey Plotnik
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.stem.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;


public class Utils {

    public static List<InetAddress> getIpAddresses() {
        try {
            List<InetAddress> addrList = new ArrayList<InetAddress>();
            Enumeration<NetworkInterface> enumNI = NetworkInterface.getNetworkInterfaces();
            while (enumNI.hasMoreElements()) {
                NetworkInterface ifc = enumNI.nextElement();
                if (ifc.isUp()) {
                    Enumeration<InetAddress> enumAdds = ifc.getInetAddresses();
                    while (enumAdds.hasMoreElements()) {
                        InetAddress addr = enumAdds.nextElement();
                        addrList.add(addr);
                    }
                }
            }
            return addrList;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static int getPortFromEndpoint(String endpoint) {
        int i = endpoint.indexOf(':');
        return Integer.valueOf(endpoint.substring(i + 1)); // TODO: validation
    }

    public static String getHostFromEndpoint(String endpoint) {
        int i = endpoint.indexOf(':');
        return endpoint.substring(0, i);
    }
}
