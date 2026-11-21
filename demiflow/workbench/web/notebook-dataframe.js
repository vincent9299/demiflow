// Preserve the original HTML output for the full Notebook view and for saving.
import {table} from './notebook-outputs.js';

export function presentDataFrame(source) {
  if (source.nextElementSibling?.classList.contains('df-dataframe-view')) return;
  const header = source.tHead;
  const body = source.tBodies[0];
  if (header?.rows.length !== 1 || !body || !body.rows.length ||
      source.tBodies.length !== 1 || source.caption || source.tFoot) return;
  const headings = Array.from(header.rows[0].cells);
  const count = headings.length;
  if (!count || count > 64 || body.rows.length > 256) return;

  // Styled HTML, linked values, truncated frames and multi-level indexes retain
  // their original renderer. Do not offer sorting over a partial data set.
  const renderedRows = [headings, ...Array.from(body.rows, row => Array.from(row.cells))];
  if (renderedRows.some(row => row.length !== count || row.some(cell =>
    cell.childElementCount || cell.colSpan !== 1 || cell.rowSpan !== 1 ||
    ['...', '…'].includes(cell.textContent.trim())))) return;

  const columns = headings.map((_, i) => String(i));
  const labels = headings.map(cell => cell.textContent);
  const values = renderedRows.slice(1).map(row => row.map(cell => cell.textContent));
  const numberText = /^-?(?:0|[1-9]\d*)(?:\.\d+)?$/;
  const numeric = columns.map(key => values.every(row => numberText.test(row[key]) &&
    Math.abs(Number(row[key])) <= Number.MAX_SAFE_INTEGER &&
    String(Number(row[key])) === row[key].replace(/(\.\d*?)0+$/, '$1').replace(/\.$/, '')));
  const sortValues = values.map(row => row.map((value, i) => numeric[i] ? Number(value) : value));
  const options = {
    columns, select: false, maxHeight: 460, layout: 'auto',
    header: labels, align: numeric.map(isNumber => isNumber ? 'right' : 'left'),
    format: columns.map(key => (_, i) => values[i][key])
  };
  const view = table(sortValues, options);
  view.classList.add('df-dataframe-view');
  if (!labels[0]) view.classList.add('df-indexed-table');
  for (const [i, th] of Array.from(view.querySelectorAll('thead th')).slice(1).entries()) {
    const label = labels[i] || '索引';
    th.title = label;
    th.tabIndex = 0;
    th.setAttribute('aria-label', label);
    th.addEventListener('click', () => {
      const mark = th.firstElementChild.textContent;
      view.querySelector('[aria-sort]')?.removeAttribute('aria-sort');
      if (mark) th.setAttribute('aria-sort', mark === '▴' ? 'ascending' : 'descending');
    });
    th.onkeydown = event => {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        event.stopPropagation();
        th.click();
      }
    };
  }
  source.after(view);
}
