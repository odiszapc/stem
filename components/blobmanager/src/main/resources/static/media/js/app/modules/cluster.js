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

define([
    '../../boilerplate',
    '../../models/storagenodestate',
    '../../collections/storagenodesstate',
    '../../models/clusterstate',
    '../../views/clusterstate_view',
    '../../app',
    'text!templates/cluster.tpl.html'
], function () {

    App.Modules.Cluster = {
        start: function () {
            var self = this;

            var src = require('text!templates/cluster.tpl.html');
            var $tpl = $('<div class="loaded-tpl"></div>')
                .html(src);
            $('#data-content').html($tpl);

            this.clusterState = new App.Models.ClusterState();

            new App.ClusterStateView({
                el: $("#cluster-state"),
                model: this.clusterState,

                progress_bar_template: $('#progress-bar-template').html(),
                progress_bar_label_template: $('#progress-bar-label-template').html(),

                cluster_state_frame_template: $('#cluster-state-frame-template').html(),
                cluster_overall_state_text_template: $('#cluster-overall-state-text-template').html(),
                space_usage_template: $('#space-usage-template').html(),

                templates: {
                    //'cluster-state': true,
                    //'cluster-capacity': true,
                    'alert': true,
                    'progress-bar': true,
                    'progress-bar-label': true,

                    'cluster-state-frame': true,
                    'cluster-overall-state-text': true,
                    'space-usage': true,
                    'storage-nodes': true,
                    'storage-node': true,
                    'disk': true
                }
            });

            this.clusterState.fetch();
            this.intervalId = setInterval(function () {
                self.clusterState.fetch();
            }, 1000);
        },

        stop: function () {
            this.clusterState = null;
            clearInterval(this.intervalId);
//            this.clusterState.get('nodes').each(function(node, a, b) {
//                node.clear();
//            });
            //this.clusterState.get('nodes').clear({silent: true});
            //this.clusterState.clear({silent: true});

        }
    };

    return App.Modules.Cluster;
});