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
    'boilerplate'

], function () {
    App.Mixins.NestedCollection = {
        _buildNestedCollection: function (src_model, params) {

            // Reconstruct
            var dataset_raw = src_model.get(params.src_field);
            src_model.unset(params.src_field, {silent: true});
            var collection = src_model.get(params.field);
            var numAdded = 0;
            var numBefore = collection.length;


            // Iterate raw dataset
            var ids_from_server = []; // ids of models received from server
            _.each(dataset_raw, function (el_raw) {
                var new_item = new params.ModelClass(el_raw, {parse: true});
                ids_from_server.push(new_item.get('id'));

                var old_item = collection.get(new_item.get('id'));

                // Itam already exist in model
                if (old_item) {
                    // Remove empty collection from new item we just created
                    new_item.unset(new_item.nested_collection_name, {silent: true});
                    old_item.set(new_item.toJSON())
                } else {
                    numAdded += 1;
                    if (numBefore == 0 && 1 == numAdded) {
                        collection.trigger(params.on_first_element_event);
                    }
                    collection.add(new_item);
                }
            }, src_model);

            collection.each(function (model) {
                if (-1 == _.indexOf(ids_from_server, model.get('id'))) {
                    collection.remove(model);
                }
            });

            var numAfter = collection.length;
            if (numBefore > 0 && numAfter == 0) {
                collection.trigger(params.on_empty_event)
            }
        }
    }
});