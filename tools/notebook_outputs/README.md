# Notebook output presentation

The data card view uses the table component and stylesheet from
[Observable Inputs](https://github.com/observablehq/inputs). The native HTML
output remains in the Notebook. The card adapter adds an interactive view for
plain, complete DataFrames. Rich, truncated or multi-level tables keep the
Jupyter renderer.

The enclosing 1px card and 8px corners follow the [marimo Gallery](https://marimo.io/gallery).
The inner output surface adapts the published-cell rules from
[marimo 0.25.2](https://github.com/marimo-team/marimo/blob/0.25.2/frontend/src/css/app/Cell.css).
Jupyter owns execution and the Python widget callbacks.

To rebuild the two browser assets from this directory, run `npm ci --ignore-scripts`
and `npm run build`. The Workbench serves the committed JS and CSS locally.
Notices for the bundled packages and marimo are in
`demiflow/workbench/web/notebook-outputs.LICENSE.txt`.
