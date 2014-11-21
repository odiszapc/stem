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

package org.stem.utils;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;


public class Utils {

    private static String machineHostname;

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

    public static int getPort(String endpoint) {
        int i = endpoint.indexOf(':');
        return Integer.valueOf(endpoint.substring(i + 1)); // TODO: validation
    }

    public static String getHost(String endpoint) {
        int i = endpoint.indexOf(':');
        return endpoint.substring(0, i);
    }

    // TODO: use BBUtils ?
    public static UUID readUuid(String path) throws IOException {
        File file = new File(path);
        if (!file.exists())
            return null;

        String uuidString = FileUtils.readFileToString(file);
        return UUID.fromString(uuidString);
    }

    public static void writeUuid(UUID id, String path) throws IOException {
        assert null != id;
        FileUtils.writeStringToFile(new File(path), id.toString());
    }

    public static String getMachineHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown";
        }
    }

    public static String listenStr(InetSocketAddress address) {
        String ip = extractHostAddr(address);

        int port = address.getPort();
        return ip + ':' + port;
    }

    public static String extractHostAddr(InetSocketAddress address) {
        if (null != address.getAddress())
            return address.getAddress().getHostAddress();
        else if (null != address.getHostName()) {
            if (address.getHostName().contains("/")) {
                int index = address.getHostName().indexOf("/");
                return address.getHostName().substring(index + 1);
            } else
                return address.getHostName();
        } else
            throw new RuntimeException("Can not extract ip address");
    }

    public static InetSocketAddress normalizeSocketAddr(InetSocketAddress address) {
        return new InetSocketAddress(extractHostAddr(address), address.getPort());
    }
}
