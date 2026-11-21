// Only the selected page is colored. The shared tree preserves multiline Python tokens.
import {parser} from '@lezer/python';
import {highlightTree, classHighlighter} from '@lezer/highlight';

export function pythonTokens(source) {
  const tree = parser.parse(source);
  return (from, to, emit) => highlightTree(tree, classHighlighter, emit, from, to);
}
