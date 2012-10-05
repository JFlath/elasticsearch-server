/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.env;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.cluster.ClusterName;

import java.io.File;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;

/**
 * The environment of where things exists.
 */
public class ClusterEnvironment extends Environment {

    private final File workWithClusterFile;

    private final File[] dataFiles;

    private final File[] dataWithClusterFiles;

    public ClusterEnvironment() {
        this(EMPTY_SETTINGS);
    }

    public ClusterEnvironment(Settings settings) {
        super(settings);
        workWithClusterFile = new File(workFile, ClusterName.clusterNameFromSettings(settings).value());

        String[] dataPaths = settings.getAsArray("path.data");
        if (dataPaths.length > 0) {
            dataFiles = new File[dataPaths.length];
            dataWithClusterFiles = new File[dataPaths.length];
            for (int i = 0; i < dataPaths.length; i++) {
                dataFiles[i] = new File(dataPaths[i]);
                dataWithClusterFiles[i] = new File(dataFiles[i], ClusterName.clusterNameFromSettings(settings).value());
            }
        } else {
            dataFiles = new File[]{new File(homeFile, "data")};
            dataWithClusterFiles = new File[]{new File(new File(homeFile, "data"), ClusterName.clusterNameFromSettings(settings).value())};
        }

    }

    /**
     * The work location with the cluster name as a sub directory.
     */
    public File workWithClusterFile() {
        return workWithClusterFile;
    }

    /**
     * The data location.
     */
    public File[] dataFiles() {
        return dataFiles;
    }

    /**
     * The data location with the cluster name as a sub directory.
     */
    public File[] dataWithClusterFiles() {
        return dataWithClusterFiles;
    }

}
