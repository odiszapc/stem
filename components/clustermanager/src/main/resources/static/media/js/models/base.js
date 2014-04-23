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
    App.Models.Base = Backbone.Model.extend({

        initialize: function (attrs) {
            this._eventDeferred = [];
        },

        _registerErrorHandler: function () {
            this.on('error', function (model, resp) {
                var response = resp.responseJSON;
                if (resp.responseJSON.errorCode) {
                    App.trigger('event:error', response.errorCode, response.error);
                }
            });
        },

        setId: function (id) {
            this.set('id', id, {silent: true});
        },

        registerDeferredEvent: function () {
            var args = Array.prototype.slice.call(arguments);
            //var event_name  = args.shift();
            this._eventDeferred.push(args);
        },

        replayDeferred: function () {
            _.each(this._eventDeferred, function (event_data) {
                this.trigger.apply(this, event_data);
            }, this);
            this._eventDeferred = [];
        }
    });
    return App.Models.Base;
});