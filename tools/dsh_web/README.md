The Workbench vendors the MIT-licensed TurnNavigator from
`@deepseek-ai/dsh-client-ui-chat@0.2.1-rc.2/lib/client.js` and the Entry / Level
tree from `@deepseek-ai/dsh-client-ui-sidebar-files@0.2.1-rc.2/lib/client.js`.
Their original CSS modules are extracted as a static stylesheet for the
Workbench content security policy. The tree adapter uses local file icons,
paged project listings and read-only previews. The rail uses the original
React virtualizer and the Workbench event window. The DSH server and tab shell
are not dependencies.

To rebuild the browser runtime, copy runtime.js, package.json and package-lock.json
into a temporary directory and run the following there. The resulting file
belongs in demiflow/workbench/web alongside dsh-runtime.LICENSE.txt.

```sh
npm ci --ignore-scripts
node_modules/.bin/esbuild runtime.js --bundle --minify --format=iife --global-name=DshRuntime --define:process.env.NODE_ENV='"production"' --outfile=dsh-runtime.min.js
```
