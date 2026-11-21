/* DSH 0.2.1-rc.2: TurnNavigator and FilesBody's Entry / Level.
MIT License

Copyright (c) 2026 DeepSeek

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
(() => {
const {react, jsx:react_jsx_runtime, virtual:_tanstack_react_virtual}=DshRuntime;
var TurnNavigator_module_css_default = {
	"dsh-turn-mark-busy": "eGxaPq_dsh-turn-mark-busy",
	"dsh-turn-preview-enter": "eGxaPq_dsh-turn-preview-enter",
	"fadeBottom": "eGxaPq_fadeBottom",
	"fadeTop": "eGxaPq_fadeTop",
	"frame": "eGxaPq_frame",
	"mark": "eGxaPq_mark",
	"markActive": "eGxaPq_markActive",
	"markBusy": "eGxaPq_markBusy",
	"markPreview": "eGxaPq_markPreview",
	"markUnloaded": "eGxaPq_markUnloaded",
	"marks": "eGxaPq_marks",
	"preview": "eGxaPq_preview",
	"previewPrompt": "eGxaPq_previewPrompt",
	"previewResponse": "eGxaPq_previewResponse",
	"scroller": "eGxaPq_scroller",
	"slot": "eGxaPq_slot"
};
const TURN_SPACING_PX = 10;
/** Rail padding above the first mark and below the last one, per end. */
const RAIL_INSET_PX = 6;
/** Fade band the mask reserves at a scrollable end. */
const FADE_PX = 24;
function preferredScrollBehavior() {
	return typeof matchMedia === "function" && matchMedia("(prefers-reduced-motion: reduce)").matches ? "auto" : "smooth";
}
const TurnMark = (0, react.memo)(function TurnMark({ item, index, active, busy, previewId, registerElement, onNavigate, onPreview, onFocusChange, t }) {
	const classes = [TurnNavigator_module_css_default.mark];
	if (item.anchor.kind === "unloaded") classes.push(TurnNavigator_module_css_default.markUnloaded);
	if (active) classes.push(TurnNavigator_module_css_default.markActive);
	else if (previewId !== void 0) classes.push(TurnNavigator_module_css_default.markPreview);
	if (busy) classes.push(TurnNavigator_module_css_default.markBusy);
	return (0, react_jsx_runtime.jsx)("button", {
		ref: registerElement,
		"data-index": index,
		type: "button",
		className: classes.join(" "),
		"aria-label": t(item.anchor.kind === "loaded" ? "chat.turnNavigation.jump" : "chat.turnNavigation.jumpLoad", { turn: item.turn }),
		"aria-current": active ? "true" : void 0,
		"aria-busy": busy ? "true" : void 0,
		"aria-describedby": previewId,
		onPointerMove: () => {
			onPreview(item.turn);
		},
		onClick: () => {
			onNavigate(item);
		},
		onFocus: () => {
			onFocusChange(item.turn);
		},
		onBlur: () => {
			onFocusChange(null);
		}
	});
});
function TurnNavigatorRail({ items, activeTurn, busyTurn, onNavigate, t }, ref) {
	const [previewTurn, setPreviewTurn] = (0, react.useState)(null);
	const [focusedTurn, setFocusedTurn] = (0, react.useState)(null);
	const scrollerRef = (0, react.useRef)(null);
	const initialization = (0, react.useRef)({
		placed: false,
		index: 0,
		follow: null,
		publishOffset: null
	});
	/** While the pointer works the rail, follow must not move it under the hand. */
	const pointerInsideRef = (0, react.useRef)(false);
	const previewId = (0, react.useId)();
	const turnIndexes = (0, react.useMemo)(() => {
		const indexes = /* @__PURE__ */ new Map();
		items.forEach((item, index) => {
			indexes.set(item.turn, index);
		});
		return indexes;
	}, [items]);
	const activeIndex = activeTurn === null ? void 0 : turnIndexes.get(activeTurn);
	(0, react.useLayoutEffect)(() => {
		initialization.current.index = activeIndex ?? 0;
	}, [activeIndex]);
	const focusedIndex = focusedTurn === null ? void 0 : turnIndexes.get(focusedTurn);
	const previewIndex = previewTurn === null ? void 0 : turnIndexes.get(previewTurn);
	const onFocusChange = (0, react.useCallback)((turn) => {
		setFocusedTurn(turn);
		setPreviewTurn(turn);
	}, []);
	const virtualizer = (0, _tanstack_react_virtual.useVirtualizer)({
		count: items.length,
		enabled: items.length >= 2,
		directDomUpdates: true,
		directDomUpdatesMode: "transform",
		useScrollendEvent: true,
		getScrollElement: (0, react.useCallback)(() => scrollerRef.current, []),
		getItemKey: (0, react.useCallback)((index) => items[index]?.turn ?? index, [items]),
		estimateSize: () => TURN_SPACING_PX,
		measureElement: () => TURN_SPACING_PX,
		initialRect: {
			width: 0,
			height: 0
		},
		initialOffset: 0,
		scrollToFn: (offset, options, instance) => {
			if (initialization.current.placed) (0, _tanstack_react_virtual.elementScroll)(offset, options, instance);
		},
		observeElementOffset: (instance, notify) => {
			initialization.current.publishOffset = notify;
			const dispose = (0, _tanstack_react_virtual.observeElementOffset)(instance, notify);
			return () => {
				dispose?.();
				initialization.current.placed = false;
				initialization.current.follow = null;
				initialization.current.publishOffset = null;
			};
		},
		observeElementRect: (instance, notify) => {
			const element = instance.scrollElement;
			const Observer = instance.targetWindow?.ResizeObserver;
			if (element === null || Observer === void 0) return;
			const observer = new Observer(([entry]) => {
				if (entry === void 0) return;
				const box = entry.borderBoxSize[0];
				const rect = {
					width: Math.round(box?.inlineSize ?? entry.contentRect.width),
					height: Math.round(box?.blockSize ?? entry.contentRect.height)
				};
				const initial = initialization.current;
				if (!initial.placed && rect.height > 0) {
					const max = Math.max(0, instance.getTotalSize() - rect.height);
					const center = initial.index * TURN_SPACING_PX + RAIL_INSET_PX;
					const target = Math.max(0, Math.min(max, center - rect.height / 2));
					initial.placed = true;
					initial.follow = {
						index: initial.index,
						count: instance.options.count,
						height: rect.height
					};
					element.scrollTop = target;
					initial.publishOffset?.(target, false);
				}
				notify(rect);
			});
			observer.observe(element, { box: "border-box" });
			return () => {
				observer.disconnect();
			};
		},
		paddingStart: RAIL_INSET_PX - TURN_SPACING_PX / 2,
		paddingEnd: RAIL_INSET_PX - TURN_SPACING_PX / 2,
		scrollPaddingStart: FADE_PX,
		scrollPaddingEnd: FADE_PX,
		overscan: 3,
		rangeExtractor: (0, react.useCallback)((range) => {
			const indexes = (0, _tanstack_react_virtual.defaultRangeExtractor)(range);
			if (focusedIndex !== void 0) {
				const last = Math.min(range.count - 1, focusedIndex + 1);
				for (let index = Math.max(0, focusedIndex - 1); index <= last; index++) if (!indexes.includes(index)) indexes.push(index);
				indexes.sort((left, right) => left - right);
			}
			return indexes;
		}, [focusedIndex])
	});
	const scrollTop = virtualizer.scrollOffset ?? 0;
	const viewHeight = virtualizer.scrollRect?.height ?? 0;
	const virtualItems = virtualizer.getVirtualItems();
	const scrollToIndex = (0, react.useCallback)((index, reveal, behavior = preferredScrollBehavior()) => {
		const item = virtualizer.measurementsCache[index];
		const height = virtualizer.scrollRect?.height ?? 0;
		if (item === void 0 || height <= 0) return;
		const current = virtualizer.scrollOffset ?? 0;
		const center = item.start + item.size / 2;
		if (reveal === "if-needed") {
			const { scrollPaddingStart, scrollPaddingEnd } = virtualizer.options;
			if (center >= current + scrollPaddingStart && center <= current + height - scrollPaddingEnd) return;
		}
		const target = center - height / 2;
		const max = Math.max(0, virtualizer.getTotalSize() - height);
		const delta = Math.max(0, Math.min(max, target)) - current;
		if (delta !== 0) virtualizer.scrollBy(delta, { behavior });
	}, [virtualizer]);
	(0, react.useImperativeHandle)(ref, () => ({
		activateTurn(turn) {
			const index = turnIndexes.get(turn);
			const item = index === void 0 ? void 0 : items[index];
			if (item !== void 0) onNavigate(item);
		},
		scrollToTurn(turn) {
			const index = turnIndexes.get(turn);
			if (index !== void 0) scrollToIndex(index, "always");
		}
	}), [
		items,
		turnIndexes,
		onNavigate,
		scrollToIndex
	]);
	(0, react.useEffect)(() => {
		if (viewHeight <= 0) {
			initialization.current.follow = null;
			return;
		}
		if (activeIndex === void 0 || pointerInsideRef.current) return;
		const previous = initialization.current.follow;
		if (previous?.index === activeIndex && previous.count === items.length && previous.height === viewHeight) return;
		initialization.current.follow = {
			index: activeIndex,
			count: items.length,
			height: viewHeight
		};
		scrollToIndex(activeIndex, "if-needed", previous?.count === items.length && previous.height === viewHeight ? preferredScrollBehavior() : "instant");
	}, [
		activeIndex,
		items.length,
		viewHeight,
		scrollToIndex
	]);
	if (items.length < 2) return null;
	const preview = previewIndex === void 0 ? void 0 : items[previewIndex];
	const previewPosition = virtualItems.find((item) => item.index === previewIndex);
	const fadeClasses = [TurnNavigator_module_css_default.scroller];
	if (scrollTop > 1) fadeClasses.push(TurnNavigator_module_css_default.fadeTop);
	if (scrollTop < virtualizer.getTotalSize() - viewHeight - 1) fadeClasses.push(TurnNavigator_module_css_default.fadeBottom);
	return (0, react_jsx_runtime.jsx)("div", {
		className: TurnNavigator_module_css_default.slot,
		children: (0, react_jsx_runtime.jsxs)("nav", {
			className: TurnNavigator_module_css_default.frame,
			"aria-label": t("chat.turnNavigation.label"),
			onPointerEnter: () => {
				pointerInsideRef.current = true;
			},
			onPointerLeave: () => {
				pointerInsideRef.current = false;
				setPreviewTurn(null);
			},
			children: [(0, react_jsx_runtime.jsx)("div", {
				ref: scrollerRef,
				className: fadeClasses.join(" "),
				children: (0, react_jsx_runtime.jsx)("div", {
					ref: virtualizer.containerRef,
					className: TurnNavigator_module_css_default.marks,
					children: virtualItems.map(({ index, key }) => {
						const item = items[index];
						if (item === void 0) return null;
						return (0, react_jsx_runtime.jsx)(TurnMark, {
							item,
							index,
							active: item.turn === activeTurn,
							busy: item.turn === busyTurn,
							previewId: item.turn === previewTurn ? previewId : void 0,
							registerElement: virtualizer.measureElement,
							onNavigate,
							onPreview: setPreviewTurn,
							onFocusChange,
							t
						}, key);
					})
				})
			}), preview !== void 0 && previewPosition !== void 0 && (0, react_jsx_runtime.jsxs)("div", {
				id: previewId,
				role: "tooltip",
				className: TurnNavigator_module_css_default.preview,
				style: { "--turn-preview-center": `${String(previewPosition.start + previewPosition.size / 2 - scrollTop)}px` },
				children: [(0, react_jsx_runtime.jsx)("div", {
					className: TurnNavigator_module_css_default.previewPrompt,
					children: preview.prompt || t("chat.turnNavigation.turn", { turn: preview.turn })
				}), preview.response !== "" && (0, react_jsx_runtime.jsx)("div", {
					className: TurnNavigator_module_css_default.previewResponse,
					children: preview.response
				})]
			})]
		})
	});
}
/**
* Fixed-pitch rail of every known Turn — loaded marks scroll, unloaded marks
* page history in first — with hover and focus previews. Overflow scrolls
* inside the frame, gradient fades marking each scrollable end, and the
* active mark centers only outside the fade-free band while the pointer is
* elsewhere. Previews follow pointer movement or focus, not scrolling under
* a stationary pointer.
*/
const TurnNavigator = (0, react.memo)((0, react.forwardRef)(TurnNavigatorRail));
var FilesBody_module_css_default = {
	"body": "k-1LKG_body",
	"fileIcon": "k-1LKG_fileIcon",
	"header": "k-1LKG_header",
	"icon": "k-1LKG_icon",
	"item": "k-1LKG_item",
	"level": "k-1LKG_level",
	"name": "k-1LKG_name",
	"note": "k-1LKG_note",
	"other": "k-1LKG_other",
	"path": "k-1LKG_path",
	"root": "k-1LKG_root",
	"row": "k-1LKG_row",
	"status": "k-1LKG_status",
	"statusLine": "k-1LKG_statusLine",
	"titleIcon": "k-1LKG_titleIcon",
	"tool": "k-1LKG_tool"
};
function childPath(parent, name) {
	return `${parent.replace(/[/\\]+$/, "")}/${name}`;
}
const byName = new Intl.Collator(void 0, {
	numeric: true,
	sensitivity: "base"
});
/**
* Order one level's entries for display: directories first, then everything
* else, each group by name. The endpoint's order is a listing fact; this is the
* reader's.
* @param entries - the listing as the endpoint returned it.
* @returns a new array, directories first, then by name within each group.
*/
function orderEntries(entries) {
	return [...entries].sort((left, right) => {
		const group = Number(right.type === "directory") - Number(left.type === "directory");
		return group !== 0 ? group : byName.compare(left.name, right.name);
	});
}
/**
* Say why a directory could not be listed, in terms of the directory.
* @param t - namespace-bound translate.
* @param failure - the settled Remote failure.
* @returns the line to show under the directory.
*/
function failureLine(t, failure) {
	switch (failure.code) {
		case "workspace-file/not-found": return t("error.notFound");
		case "workspace-file/outside-workspace": return t("error.outsideWorkspace");
		case "workspace-file/not-directory": return t("error.notDirectory");
		default: return t("error.unavailable", { message: failure.message });
	}
}
/** One entry's row, and its children when it is an expanded directory. */
function Entry({ parent, entry, tree }) {
	const path = childPath(parent, entry.name);
	if (entry.type === "directory") {
		const expanded = tree.state.expanded.includes(path);
		return (0, react_jsx_runtime.jsxs)("li", {
			className: FilesBody_module_css_default.item,
			"data-files-entry": "directory",
			"data-files-path": path,
			children: [(0, react_jsx_runtime.jsxs)("button", {
				type: "button",
				className: FilesBody_module_css_default.row,
				"aria-expanded": expanded,
				onClick: () => {
					tree.onToggle(parent, path);
				},
				children: [expanded ? (0, react_jsx_runtime.jsx)(_deepseek_ai_dsh_client_ui_primitives.IconFolderOpenRegular, { className: FilesBody_module_css_default.icon }) : (0, react_jsx_runtime.jsx)(_deepseek_ai_dsh_client_ui_primitives.IconFolderCloseRegular, { className: FilesBody_module_css_default.icon }), (0, react_jsx_runtime.jsx)("span", {
					className: FilesBody_module_css_default.name,
					children: entry.name
				})]
			}), expanded && (0, react_jsx_runtime.jsx)("ul", {
				className: FilesBody_module_css_default.level,
				children: (0, react_jsx_runtime.jsx)(Level, {
					path,
					tree
				})
			})]
		});
	}
	if (entry.type === "file") return (0, react_jsx_runtime.jsx)("li", {
		className: FilesBody_module_css_default.item,
		"data-files-entry": "file",
		"data-files-path": path,
		children: (0, react_jsx_runtime.jsxs)(entry.preview ? "button" : "span", {
			type: entry.preview ? "button" : undefined,
			className: FilesBody_module_css_default.row,
			onClick: () => {
				if (entry.preview) tree.onOpen(path);
			},
			children: [(0, react_jsx_runtime.jsx)(_deepseek_ai_dsh_client_ui_primitives.FileTypeIcon, {
				kind: (0, _deepseek_ai_dsh_client_ui_primitives.classifyFileType)(entry.name),
				size: 16,
				className: FilesBody_module_css_default.fileIcon
			}), (0, react_jsx_runtime.jsx)("span", {
				className: FilesBody_module_css_default.name,
				children: entry.name
			})]
		})
	});
	return (0, react_jsx_runtime.jsx)("li", {
		className: FilesBody_module_css_default.item,
		"data-files-entry": "other",
		"data-files-path": path,
		children: (0, react_jsx_runtime.jsx)("span", {
			className: [FilesBody_module_css_default.row, FilesBody_module_css_default.other].join(" "),
			"aria-disabled": "true",
			title: tree.t("entry.other"),
			children: (0, react_jsx_runtime.jsx)("span", {
				className: FilesBody_module_css_default.name,
				children: entry.name
			})
		})
	});
}
/** One directory's rows: its state while listing, its entries once listed. */
function Level({ path, tree }) {
	const { state, t } = tree;
	const level = state.levels[path];
	if (level === void 0 || level.kind === "loading") return (0, react_jsx_runtime.jsx)("li", {
		className: FilesBody_module_css_default.note,
		"data-files-row": "loading",
		children: t("loading")
	});
	if (level.kind === "failed") return (0, react_jsx_runtime.jsx)("li", {
		className: FilesBody_module_css_default.note,
		"data-files-row": "failed",
		"data-files-code": level.failure.code,
		children: failureLine(t, level.failure)
	});
	const entries = orderEntries(level.level.entries);
	return (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
		level.failure !== void 0 && (0, react_jsx_runtime.jsx)("li", {
			className: FilesBody_module_css_default.note,
			"data-files-row": "failed",
			children: failureLine(t, level.failure)
		}),
		entries.length === 0 && (0, react_jsx_runtime.jsx)("li", {
			className: FilesBody_module_css_default.note,
			"data-files-row": "empty",
			children: t("empty")
		}),
		entries.map((entry) => (0, react_jsx_runtime.jsx)(Entry, {
			parent: path,
			entry,
			tree
		}, entry.name)),
		level.level.truncated && (0, react_jsx_runtime.jsx)("li", {
			className: FilesBody_module_css_default.note,
			"data-files-row": "truncated",
			children: react.createElement("button", {type:"button", onClick:()=>tree.onMore(path)}, t("truncated"))
		})
	] });
}

// DemiFlow supplies the three icon primitives used by these imported rows.
function fileGlyph(path) {
  return ({className}) => react.createElement('svg', {className, width:16, height:16,
    viewBox:'0 0 24 24', fill:'none', stroke:'currentColor', strokeWidth:1.5, 'aria-hidden':true},
    react.createElement('path', {d:path}));
}
const _deepseek_ai_dsh_client_ui_primitives = {
  IconFolderCloseRegular:fileGlyph('M2 5h8l2 2h10v13H2z M5 11h13'),
  IconFolderOpenRegular:fileGlyph('M2 18V5h8l2 2h9v3 M2 20l3-9h17l-3 9z'),
  FileTypeIcon:fileGlyph('M5 2h9l5 5v15H5z M14 2v6h5'),
  classifyFileType:()=> 'file'
};
globalThis.DshWorkbench = {TurnNavigator, Level, files:FilesBody_module_css_default};
})();
