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
    '../models/base',
    '../collections/disksstate',
    '../mixins/storage_entity'
], function () {
    App.Models.StorageNodeState = App.Models.Base.extend({

        defaults: function () {
            return {
                disks: new App.DisksStateCollection([], {parse: true})
            }
        },

        initialize: function (attrs) {
            this.nested_collection_name = 'disks';

            //this.set('disks', App.DisksStateCollection());

            this.setId('node-' + attrs['endpoint'].replace(':', '-'));
            this.get('disks').storage_node = this;

            this.on('change', function (model) {
                //console.log('node changed : ' + model.get('id') + ', changed attributes: ' + JSON.stringify(model.changedAttributes()))
            });

            this.get('disks').on('add', function (model) {
                //console.log('disk added')
            });
        },

        parse: function (resp) {
            resp['disks_raw'] = resp['disks'];
            delete resp['disks'];
            return resp;
        }
    });

    _.extend(App.Models.StorageNodeState.prototype, App.Mixins.StorageEntity);

    return App.Models.StorageNodeState;
});