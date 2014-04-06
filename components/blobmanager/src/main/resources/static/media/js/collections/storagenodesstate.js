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
    '../mixins/nested_collection'

], function () {
    App.StorageNodesStateCollection = Backbone.Collection.extend({
        model: App.Models.StorageNodeState,

        initialize: function () {
            this.on('change', function (model) {
                //console.log('MODEL: collection has changed : ' + model.get('id') + ', changed attributes: ' + JSON.stringify(model.changedAttributes()))
            });
            this.on('change', this.nodeChanged);
            this.on('reset', function () {
                //console.log('MODEL: collection reset')
            });
            this.on('add', this.nodeAdded);
            this.on('remove', function (model) {
                //console.log('MODEL: node removed : ' + model.get('id'))
            });
            this.on('first-node-in-cluster', function (model) {
                //console.log('MODEL: first-node-in-cluster')
            });
        },

        nodeAdded: function (node) {
            this.trigger('node:added', node);
            this._buildDisksCollection(node);
        },

        nodeChanged: function (node) {
            this._buildDisksCollection(node);
        },

        _buildDisksCollection: function (node) {
            this._buildNestedCollection(node, {
                src_field: 'disks_raw',
                field: 'disks',
                ModelClass: App.Models.DiskState,
                on_first_element_event: 'first-disk-on-node',
                on_empty_event: 'node:exhausted'
            });
        },

        parse: function (resp) {

        }
    });

    _.extend(App.StorageNodesStateCollection.prototype,
        App.Mixins.NestedCollection);

    return App.StorageNodesStateCollection;
})
;