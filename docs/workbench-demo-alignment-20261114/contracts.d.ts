// UI-D1.9/1. Design-only declarations; no TypeScript build dependency is required.
// HTTP objects not expanded here retain their source schema, not an `any` protocol.
type JSONValue = null | boolean | number | string | readonly JSONValue[] | {readonly [key: string]: JSONValue};
type SHA256 = string; // exactly 64 lowercase hex, validate at runtime
type ProjectId = string; // ^[wl][0-9a-f]{32}$
type ReportNodeId = string;
interface RawReportNode {
  readonly id:ReportNodeId; readonly kind:string; readonly api:string;
  readonly source:string; readonly line:number;
  readonly operator:Readonly<Record<string,JSONValue>>|null;
  readonly fields:Readonly<Record<string,JSONValue>>;
  readonly parameters:Readonly<Record<string,JSONValue>>;
  readonly normalized_parameters?:Readonly<Record<string,JSONValue>>;
  readonly parameter_origins?:Readonly<Record<string,JSONValue>>;
}
type GraphNodeKey = string; // JSON tuple described in C2, never an engine identity
type UnknownReason = string;
interface GraphBinding {
  readonly workspace_id: ProjectId;
  readonly basis_kind: 'source' | 'report' | 'run';
  readonly basis_digest: SHA256;
  readonly entry: string | null;
  readonly params: string | null;
  readonly run_id: string | null;
  readonly attempt: number | null;
}
interface NodeSelection {
  readonly key: GraphNodeKey;
  readonly reportNodeId: ReportNodeId | null;
  readonly actionId: string | null;
  readonly logicalNodeId: string | null;
  readonly occurrenceId: string | null;
}
interface GraphChoice {
  readonly kind: 'source' | 'report' | 'sealed' | 'unavailable';
  readonly actionId: string | null;
  readonly label: string;
  readonly reason: UnknownReason | null;
}
interface FactLine {
  readonly label: string; readonly value: string | null;
  readonly basis: 'report' | 'source' | 'sealed' | 'observation';
  readonly reason: UnknownReason | null;
}
interface NodeCard {
  readonly key: GraphNodeKey; // visual node/group key, also used by GraphEdge and layout
  readonly selection: NodeSelection;
  readonly occurrences: readonly NodeSelection[]; // sealed members; empty for report/source
  readonly kindLabel: string; readonly title: string;
  readonly summary: readonly FactLine[]; // <=2, bounded strings, never fabricated
  readonly state: {readonly label: string; readonly tone: 'neutral'|'success'|'failed'|'running'};
}
interface GraphEdge {
  // Current DAG endpoints contain logical IDs + ports, not occurrence endpoints.
  // Sealed edges connect logical groups. Never invent per-occurrence cross edges.
  readonly from: GraphNodeKey; readonly to: GraphNodeKey;
  readonly kind: 'data'|'value'|'action'|'reference';
  readonly fromPort: string|null; readonly toPort: string|null;
}
interface GraphModel {
  readonly binding: GraphBinding; readonly choice: GraphChoice;
  readonly nodes: readonly NodeCard[]; readonly edges: readonly GraphEdge[];
  readonly selected: GraphNodeKey|null;
  readonly completeness: {readonly display: string; readonly totalNodes:number; readonly totalEdges:number};
  readonly notices: readonly string[];
}
interface NodeDetails {
  readonly selection:NodeSelection; readonly title:string;
  readonly inputs:readonly FactLine[]; readonly outputs:readonly FactLine[];
  readonly parameters:readonly FactLine[];
  readonly sourceAvailable:boolean; readonly sourceReason:string|null;
  readonly recoveryRestriction:string|null;
}
// RawGraph and RawSnapshot are validated outputs of graph/model.py::get and runs/view.
// They are passed through, never persisted by this module or reconstructed from cards.
type RawGraph = Readonly<Record<string, JSONValue>>;
type RawSnapshot = Readonly<Record<string, JSONValue>>;
interface PositionedNode {readonly key:GraphNodeKey;readonly x:number;readonly y:number;readonly width:number;readonly height:number;}
interface GraphLayout {readonly nodes:readonly PositionedNode[];readonly width:number;readonly height:number;}
interface GraphController {
  update(model:GraphModel):void;
  select(key:GraphNodeKey|null):void;
  fit():void;
  zoom(factor:number):void;
  dispose():void;
}
interface WorkbenchPresentation {
  chooseGraphView(graph:RawGraph,options:{actionId:string|null;preference:'auto'|'report'}):GraphChoice;
  buildNodeCard(graph:RawGraph,selection:NodeSelection,snapshot:RawSnapshot|null):NodeCard;
  buildGraphModel(graph:RawGraph,options:{choice:GraphChoice;snapshot:RawSnapshot|null;selection:NodeSelection|null}):GraphModel;
  buildNodeDetails(graph:RawGraph,selection:NodeSelection):NodeDetails;
}
interface VersionLocator {
  readonly kind:'workspace_table'; readonly alias:string;
  readonly binding:SHA256; readonly columns:readonly string[];
  readonly version:number; readonly alias_binding:SHA256;
}
interface CurrentLocator {
  readonly kind:'workspace_table'; readonly alias:string;
  readonly binding:SHA256; readonly columns:readonly string[];
}
interface ProjectLocator {
  readonly kind:'project_table'; readonly id:string; readonly discovery:SHA256;
  readonly version:number; readonly binding:SHA256; readonly columns:readonly string[];
}
type SourceLocator = CurrentLocator|VersionLocator|ProjectLocator;
interface DataSelection {
  readonly project:ProjectId; readonly locator:SourceLocator;
  readonly label:string; readonly title:string;
  readonly version:number|null; readonly rows:number|null; readonly fields:number|null;
}
interface CellRunOutcome {
  readonly kind:'submitted'|'pending'|'blocked'|'cancelled';
  readonly requestId:string|null; readonly executionId:string|null;
  readonly message:string;
}
interface SaveOutcome {readonly kind:'saved'|'pending'|'conflict';readonly notebookId:string;readonly revision:number;}
interface RunCapture {
  readonly project:ProjectId; readonly epoch:number;
  readonly notebookId:string; readonly revision:number;
  readonly cell:NotebookCell; readonly cellSha:SHA256;
  readonly sources:Readonly<Record<string,SHA256>>;
  readonly sessionId:string|null; readonly generation:number|null;
}
type SessionOutcome =
  | {readonly kind:'ready';readonly sessionId:string;readonly generation:number}
  | {readonly kind:'pending'|'blocked';readonly requestId:string|null;readonly message:string};
interface AnalysisToolbarHosts {readonly notebook:HTMLElement;readonly case:HTMLElement;}
type CellLanguage='python'|'sql'|'markdown';
interface NotebookCell {readonly id:string;readonly language:CellLanguage;readonly source:string;readonly result_name?:string;}
interface FieldHandle {readonly element:HTMLElement;readonly ready?:Promise<unknown>|null;dispose():void;}
interface FieldPresentation {density?:'normal'|'compact';tools?:'inline'|'menu';}
// Addition to existing ForgeAnalysisRender.field options, not a replacement for
// resolveMedia/preview/mountText/isCurrent or their fixed-source ownership.
type Scalar = null|boolean|number|string;
interface TableSlice {
  readonly columns:readonly {readonly name:string;readonly type:string}[]; // <=16
  readonly rows:readonly Readonly<Record<string,Scalar>>[]; // <=200 and <=256KiB
  readonly coverage:{readonly label:string;readonly complete:boolean;readonly totalRows:number|null};
  readonly identity:Readonly<{project:ProjectId;binding:SHA256;artifactDigest?:SHA256;executionId?:string;outputId?:string;outputIndex?:number;sourceSnapshot?:SHA256}>;
}
interface ChartSpec {readonly kind:'table'|'bar'|'line'|'json';readonly x:string|null;readonly y:string|null;}
interface Charts {
  validateChart(slice:TableSlice,spec:ChartSpec):{readonly allowed:boolean;readonly reason:string|null};
  mount(options:{parent:HTMLElement;slice:TableSlice;spec:ChartSpec;isCurrent:()=>boolean}):FieldHandle;
}
interface ViewportPolicy {readonly mode:'wide'|'medium'|'narrow';readonly showFilters:boolean;readonly showReview:boolean;}
interface CaseTitleInput {readonly fields:readonly {readonly field:string;readonly role:string;readonly format:string}[];}
// Existing Case descriptor wire protocol is unchanged; case_view() remains authority.
interface CaseGroup {readonly id:string;readonly label:string;readonly fields:readonly string[];}
interface VersionResolutionWire {
  readonly version:number;
  readonly alias_binding:SHA256;
  readonly binding:Readonly<Record<string,JSONValue>>; // exact preview_binding() object
  readonly reference:Readonly<Record<string,JSONValue>>; // existing control with fixed ref
  readonly metadata:Readonly<Record<string,JSONValue>>; // preview.describe_table()
}
declare const ForgeWorkbenchPresentation:WorkbenchPresentation;
declare const ForgeAnalysisCharts:Charts;
