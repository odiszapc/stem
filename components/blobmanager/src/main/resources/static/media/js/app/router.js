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
    'boilerplate',

], function () {

    App.Router = Backbone.Router.extend({
        normalize_route: function (route) {
            if (_.isEmpty(route)) {
                route = "root";
            }
            return route;
        }
    });

    // Add :before :after events support to router
    App.Router.prototype.before = function () {
    };
    App.Router.prototype.after = function () {
    };

    App.Router.prototype.route = function (route, name, callback) {
        if (!_.isRegExp(route)) route = this._routeToRegExp(route);
        if (_.isFunction(name)) {
            callback = name;
            name = '';
        }
        if (!callback) callback = this[name];

        var router = this;

        Backbone.history.route(route, function (fragment) {
            var args = router._extractParameters(route, fragment);

            router.before.apply(router, arguments);
            callback && callback.apply(router, args);
            router.after.apply(router, arguments);

            router.trigger.apply(router, ['route:' + name].concat(args));
            router.trigger('route', name, args);
            Backbone.history.trigger('route', router, name, args);
        });
        return this;
    };

    return App.Router;
});