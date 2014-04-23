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

function () {
    function k(a) {
        var b = a.currentTarget || a.srcElement, c;
        if (a.type === "load" || l.test(b.readyState)) {
            a = b.getAttribute("data-requiremodule");
            j[a] = !0;
            for (a = 0; c = g[a]; a++)if (j[c.name])c.req([c.name], c.onLoad); else break;
            a > 0 && g.splice(0, a);
            setTimeout(function () {
                b.parentNode.removeChild(b)
            }, 15)
        }
    }

    function m(a) {
        var b, c;
        a.setAttribute("data-orderloaded", "loaded");
        for (a = 0; c = h[a]; a++)if ((b = i[c]) && b.getAttribute("data-orderloaded") === "loaded")delete i[c], require.addScriptToDom(b); else break;
        a > 0 && h.splice(0,
            a)
    }

    var f = typeof document !== "undefined" && typeof window !== "undefined" && document.createElement("script"), n = f && (f.async || window.opera && Object.prototype.toString.call(window.opera) === "[object Opera]" || "MozAppearance"in document.documentElement.style), o = f && f.readyState === "uninitialized", l = /^(complete|loaded)$/, g = [], j = {}, i = {}, h = [], f = null;
    define({version: "1.0.5", load: function (a, b, c, e) {
        var d;
        b.nameToUrl ? (d = b.nameToUrl(a, null), require.s.skipAsync[d] = !0, n || e.isBuild ? b([a], c) : o ? (e = require.s.contexts._, !e.urlFetched[d] && !e.loaded[a] && (e.urlFetched[d] = !0, require.resourcesReady(!1), e.scriptCount += 1, d = require.attach(d, e, a, null, null, m), i[a] = d, h.push(a)), b([a], c)) : b.specified(a) ? b([a], c) : (g.push({name: a, req: b, onLoad: c}), require.attach(d, null, a, k, "script/cache"))) : b([a], c)
    }})
}
)
();
