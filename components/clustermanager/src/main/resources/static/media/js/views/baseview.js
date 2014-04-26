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
    /**
     * Base View
     */
    App.BaseView = Backbone.View.extend({

        initialize: function (attrs) {
            // Init templates
            this.templates = {};
            _.each(attrs.templates, function (params, template_name) {
                this.templates[template_name] = _.template($('#' + template_name + '-template').html());
            }, this);
        },

        getRenderedTemplate: function (name) {
            return this.$('#' + name);
        },

        isOnPage: function (name) {
            return this.has('#' + name);
        },

        has: function (selector) {
            return this.$(selector).length > 0;
        },

        buildFromTemplate: function (name, params) {
            var id_suffix = '';
            if (params) {
                if (params.id) {
                    id_suffix = '-' + params.id;
                }
            }

            var rendered = this.render_template(name, params);
            return $('<div></div>')
                .attr('id', name + id_suffix)
                .addClass('tpl-' + name)
                .html(rendered);
        },

        render_template: function (name, params) {
            if (null == params) {
                params = {};
            }

            return this.templates[name](params)
        },


        replaceRenderedTemplate: function (name, newElement) {
            if (this.isOnPage(name)) {
                this.getRenderedTemplate(name).replaceWith(newElement);
                return true;
            }
            return false;
        },

        hideAndRemove: function ($el, speed) {
            if (!speed) {
                speed = 'fast';
            }
            $el.fadeOut(speed, function () {
                this.remove();
            });
        }
    });

    //return App.ClusterStateView;
});