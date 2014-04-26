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
    App.Mixins.StorageEntity = {
        // TODO: take out below to mixin #1
        usedHuman: function () {
            return this._bytesToSize(this.get('usedBytes'), 2);
        },

        totalHuman: function () {
            return this._bytesToSize(this.get('totalBytes'), 2);
        },

        _bytesToSize: function (bytes, precision) {
            var sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
            if (bytes == 0) return '0 Bytes';
            var i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)));
            var precision_scale = Math.pow(10, precision);
            return Math.round(bytes / Math.pow(1024, i) * precision_scale) / precision_scale + ' ' + sizes[i];
        },

        usedPercentage: function () {
            if (0 == this.get('totalBytes'))
                return 0;
            return Math.round(this.get('usedBytes') / this.get('totalBytes') * 100 * 100) / 100;
        }
        // TODO: take out below to mixin #1
    }
});