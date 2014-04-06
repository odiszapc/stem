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
    'app/router',
    'views/ui_view',
    'app/modules/root',
    'app/modules/cluster',
    'app/modules/topology',

], function () {

    App.UI = new App.UIView({
        el: $('body')
    });

    var initBackBoneRouter = function () {
        App.Router = new (App.Router.extend({
            routes: {
                '': 'root',
                'cluster': 'cluster',
                'topology': 'topology'
            },

            root: function () {
                $('#data-content').empty();
            },

            topology: function () {
                $('#data-content').empty();
            },

            cluster: function () {

                $('#data-content').empty();

                App.startModule('cluster');
            },

            before: function (route) {
                route = this.normalize_route(route);
                var prev_route = this.active_route;
                if (prev_route && prev_route != route) {
                    // User leave {prev_route}
                    var module = require('app/modules/' + prev_route);
                    if (module.stop)
                        module.stop();
                }
                App.UI.hideAlert();

                require('app/modules/' + route);
            },

            after: function (route) {
                route = this.normalize_route(route);
                this.active_route = route;
            }
        }))();

        // Invoked when navigate to any route
        App.Router.bind("all", function (route, router) {
            if (-1 != route.indexOf(':')) {
                route = route.substr(route.indexOf(':') + 1);
                if (route == "") route = "root";
                var anchor = $(".menu-item.r-" + route);
                $(".menu-item").removeClass("active");
                anchor.addClass("active");
            }
        });

        Backbone.history.start({pushState: false}); // TRUE on Production ?
    };

    initBackBoneRouter();

    $(document).on("click", "a:not([data-bypass])", function (e) {
        var href = { prop: $(this).prop("href"), attr: $(this).attr("href") };
        var root = location.protocol + "//" + location.host + "/";

        if (href.prop && href.prop.slice(0, root.length) === root) {
            e.preventDefault();
            Backbone.history.navigate(href.attr, true);
        }
    });

    return {};
});