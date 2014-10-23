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

package org.stem;


import org.stem.service.StemDaemon;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class NodeSupervisor extends Thread
{
    private ServerSocket serverSocket;

    public NodeSupervisor() throws IOException
    {
        serverSocket = new ServerSocket(19010, 1, InetAddress.getByName("localhost"));
        serverSocket.setReuseAddress(true);
    }

    @Override
    public void run()
    {
        while (serverSocket != null)
        {
            Socket socket = null;
            try
            {
                socket = serverSocket.accept();
                socket.setSoLinger(false, 0);
                LineNumberReader lin = new LineNumberReader(new InputStreamReader(socket.getInputStream()));
                String key = lin.readLine();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
            finally
            {
                if (socket != null)
                {
                    try
                    {
                        socket.close();
                    }
                    catch (IOException e)
                    {
                        // ignore
                    }
                }
                socket = null;
            }
        }
    }

    public static void main(String[] args) throws IOException
    {
        NodeSupervisor supervisor = new NodeSupervisor();
        supervisor.setDaemon(true);
        supervisor.start();
        StemDaemon.main(args);
    }
}
