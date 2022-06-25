/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.ipc.security;

import java.io.File;
import java.security.KeyStore;

import org.apache.hyracks.api.network.INetworkSecurityConfig;

public class NetworkSecurityConfig implements INetworkSecurityConfig {

    private static final long serialVersionUID = -1914030130038989199L;
    private final boolean sslEnabled;
    private final File keyStoreFile;
    private final File trustStoreFile;
    private final String keyStorePassword;

    private NetworkSecurityConfig(boolean sslEnabled, String keyStoreFile, String keyStorePassword,
            String trustStoreFile) {
        this.sslEnabled = sslEnabled;
        this.keyStoreFile = keyStoreFile != null ? new File(keyStoreFile) : null;
        this.keyStorePassword = keyStorePassword;
        this.trustStoreFile = trustStoreFile != null ? new File(trustStoreFile) : null;
    }

    public static NetworkSecurityConfig of(boolean sslEnabled, String keyStoreFile, String keyStorePassword,
            String trustStoreFile) {
        return new NetworkSecurityConfig(sslEnabled, keyStoreFile, keyStorePassword, trustStoreFile);
    }

    @Override
    public boolean isSslEnabled() {
        return sslEnabled;
    }

    @Override
    public File getKeyStoreFile() {
        return keyStoreFile;
    }

    @Override
    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    @Override
    public KeyStore getKeyStore() {
        return null;
    }

    @Override
    public KeyStore getTrustStore() {
        return null;
    }

    @Override
    public File getTrustStoreFile() {
        return trustStoreFile;
    }
}
