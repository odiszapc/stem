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
    '../mixins/storage_entity'
], function () {

    App.Models.DiskState = Backbone.Model.extend({
        getStorageNode: function () {
            return this.collection.storage_node;
        }
    });

    _.extend(App.Models.DiskState.prototype, App.Mixins.StorageEntity);

    return App.Models.DiskState;
});