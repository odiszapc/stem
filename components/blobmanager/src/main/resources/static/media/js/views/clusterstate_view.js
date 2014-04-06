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
    'views/baseview'
], function () {
    /**
     * View
     */
    App.ClusterStateView = App.BaseView.extend({
        initialize: function (attrs) {
            App.BaseView.prototype.initialize(attrs);

            var self = this;
            this.opts = attrs;
            //this.template = _.template(options.cluster_state_template);
            //this.cluster_capacity_template = _.template(options.cluster_capacity_template);
            this.progress_bar_template = _.template(attrs.progress_bar_template);
            this.progress_bar_label_template = _.template(attrs.progress_bar_label_template);

            this.cluster_state_frame_template = _.template(attrs.cluster_state_frame_template);
            this.cluster_overall_state_text_template = _.template(attrs.cluster_overall_state_text_template);
            this.space_usage_template = _.template(attrs.space_usage_template);


            this.notRenderedYet = true;

            this.model
                .on('data-received', this.render, this);

            this.model.get('nodes')
                .on('first-node-in-cluster', this.onFirstNodeAdded, this)
                .on('node:added', this.onNodeAdded, this)
                .on('change', this.onNodeChanged, this)
                .on('remove', this.onNodeRemoved, this)
                .on('cluster:exhausted', this.onLastNodeRemoved, this)
            ;
            //.on('first-disk-on-node', this.onFirstDiskAdded, this)
        },


        onNodeChanged: function (node) {
            //console.log('node changed: ' + node.get('totalBytes'));
            this.updateProgressBar({
                id: 'progress-bar-' + node.get('id'),
                model: node
            });
        },


        onFirstDiskAdded: function () {
        },

        onDiskAdded: function (disk) {

            var $disk = this.buildFromTemplate('disk', {
                id: disk.get('id'),
                path: disk.get('path')
            });

            this.getRenderedTemplate('storage-node-' + disk.getStorageNode().get('id'))
                .find('.panel-body')
                .append($disk);

            $disk.find('.progress-container').append(
                this.buildProgressBar(disk)
            );

            var $label = this.buildCapacityLabel(disk);
            this.getRenderedTemplate('progress-bar-' + disk.get('id'))
                .after($label);
        },

        onDiskChanged: function (disk) {
            this.updateProgressBar({
                id: 'progress-bar-' + disk.get('id'),
                model: disk
            });

            this.replaceRenderedTemplate('space-usage-' + disk.get('id'),
                this.buildCapacityLabel(disk)
            );
        },

        onDiskRemoved: function (disk) {
            this.hideAndRemove(
                this.getRenderedTemplate('disk-' + disk.get('id')));
        },

        onLastDiskRemoved: function () {
            // Nothing, because we have no container for disks
        },

        onFirstNodeAdded: function () {
            this.$cluster_state_frame = this.buildFromTemplate('storage-nodes');
            this.$cluster_state_frame_panel_body.append(this.$cluster_state_frame);
        },

        onNodeAdded: function (node) {
            node.get('disks')
                .on('add', this.onDiskAdded, this)
                .on('change', this.onDiskChanged, this)
                .on('first-disk-on-node', this.onFirstDiskAdded, this)
                .on('remove', this.onDiskRemoved, this)
                .on('node:exhausted', this.onLastDiskRemoved, this);

            var $storage_node_el =
                this.buildFromTemplate('storage-node', {
                    id: node.get('id'),
                    endpoint: node.get('endpoint')
                });

            this.getRenderedTemplate('storage-nodes')
                .append($storage_node_el);

            $storage_node_el
                .find('.panel-body')
                .append(this.buildProgressBar(node));
        },

        onNodeRemoved: function (node) {
            this.hideAndRemove(
                this.getRenderedTemplate('storage-node-' + node.get('id')));
        },

        onLastNodeRemoved: function () {
            console.log('last node removed');

            this.hideAndRemove(this.$cluster_state_frame);
        },

        render: function () {
            this.renderFrame();
            this
                .renderClusterStatusText()
                .renderGlobalProgressBar({
                    model: this.model,
                    id: 'cluster-overall-state-progress-bar'
                });
        },

        renderFrame: function () {
            if (this.isOnPage('cluster-state-frame'))
                return;

            this.$cluster_state_frame = this.buildFromTemplate('cluster-state-frame');
            this.$cluster_state_frame_panel_body = this.$cluster_state_frame.find('.panel-body');
            this.$el.append(this.$cluster_state_frame);
        },

        renderClusterStatusText: function () {
            this.$cluster_overall_state_text = this.buildFromTemplate('cluster-overall-state-text', {
                'space_usage_template': this.render_template('space-usage', {
                    percentage: this.model.usedPercentage(),
                    usedHuman: this.model.usedHuman(),
                    used: this.model.get('usedBytes'),
                    totalHuman: this.model.totalHuman(),
                    total: this.model.get('totalBytes')
                })
            });

            this.replaceRenderedTemplate('cluster-overall-state-text', this.$cluster_overall_state_text)
            || this.$cluster_state_frame_panel_body
                .find('h4')
                .after(this.$cluster_overall_state_text);

            return this;
        },

        renderGlobalProgressBar: function (params) {
            var id = params.id;
            if (this.isOnPage(id)) {
                this.updateProgressBar(params);
            } else {
                this.$cluster_overall_state_progress_bar = $(this.renderProgressBarTemplate(params));
                this.$cluster_state_frame_panel_body.append(this.$cluster_overall_state_progress_bar);
            }
            return this
        },

        renderProgressBarTemplate: function (params) {
            return this.render_template('progress-bar', {
                id: params.id,
                used_percentage: this.model.usedPercentage(),
                progress_bar_label_template: this.progress_bar_label_template(this.model)
            })
        },

        updateProgressBar: function (params) {
            var id_selector = '#' + params.id;
            if (!this.has(id_selector))
                return;

            var progress = this.$(id_selector)
                .find('.progress-bar');

            progress.attr('aria-valuenow', params.model.usedPercentage());
            progress.width(params.model.usedPercentage() + '%');
            progress
                .find('span')
                .html(this.progress_bar_label_template(params.model))
        },

        buildProgressBar: function (model) {
            return this.buildFromTemplate('progress-bar', {
                id: model.get('id'),
                used_percentage: model.usedPercentage(),
                aria_valuenow: Math.round(model.get('usedBytes') / model.get('totalBytes') * 100),
                progress_bar_label_template: this.progress_bar_label_template(model)
            })
        },

        buildCapacityLabel: function (model) {
            return this.buildFromTemplate('space-usage', {
                id: model.get('id'),
                percentage: model.usedPercentage(),
                usedHuman: model.usedHuman(),
                used: model.get('usedBytes'),
                totalHuman: model.totalHuman(),
                total: model.get('totalBytes')
            })
        },

        renderStorageNodes: function () {
            if (this.isOnPage('storage-nodes'))
                return;

            this.$cluster_state_frame = this.buildFromTemplate('storage-nodes');
            this.$cluster_state_frame_panel_body.append(this.$cluster_state_frame);
            this.model.get('nodes').forEach(function (node) {
                //console.log(node.get('endpoint'));
            })
        },


        // Base class

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
        }
    });

    //return App.ClusterStateView;
});