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
    '../boilerplate',
    '../models/storagenodestate',
    '../collections/storagenodesstate',
    '../mixins/storage_entity',
    '../mixins/nested_collection'

], function () {

    /**
     * Model ClusterState
     */
    App.Models.ClusterState = App.Models.Base.extend({
        url: "/api/cluster",

        defaults: function () {
            return {
                nodes: new App.StorageNodesStateCollection([], {parse: true})
            }
        },

        initialize: function (attrs) {
            this.nested_collection_name = 'nodes';
            //this.set('nodes', new App.StorageNodesStateCollection([], {parse: true}));

            this._registerErrorHandler();
            this.on('sync', this.onSync);
        },

        onSync: function () {
            App.trigger('event:sync');

            this.trigger('data-received');

            this._buildNestedCollection(this, {
                src_field: 'nodes_raw',
                field: 'nodes',
                ModelClass: App.Models.StorageNodeState,
                on_first_element_event: 'first-node-in-cluster',
                on_empty_event: 'cluster:exhausted'
            });
        },

        parse: function (resp) {
            var cluster = resp['cluster'];

            cluster['nodes_raw'] = cluster['nodes'];
            delete cluster['nodes'];

            return cluster;
        }
    });

    _.extend(App.Models.ClusterState.prototype,
        App.Mixins.StorageEntity,
        App.Mixins.NestedCollection);

    return App.Models.ClusterState;
});