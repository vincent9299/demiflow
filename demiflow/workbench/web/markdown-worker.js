/* Fixed same-origin bootstrap. reading.js owns the shared bounded parser logic.
 * No HTTP client, resource resolver, project state, credentials or retries. */
'use strict';
importScripts('/markdown-it.min.js', '/reading.js');
