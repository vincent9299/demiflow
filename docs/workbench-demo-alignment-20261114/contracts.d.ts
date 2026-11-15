// UI-D1.9/2. Design-only declarations; no TypeScript build dependency is required.
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
  readonly execution: ExecutionViewIdentity | null; // source/report = null
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
  // Legacy edges connect logical groups. v3 edges connect exact occurrences.
  readonly edgeId: string|null; // native edge_id when present; never invent a legacy engine ID
  readonly from: GraphNodeKey; readonly to: GraphNodeKey;
  readonly kind: 'data'|'value'|'action'|'reference';
  readonly fromPort: string|null; readonly toPort: string|null;
}
interface GraphModel {
  readonly binding: GraphBinding; readonly choice: GraphChoice;
  readonly execution: ExecutionViewIdentity|null;
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
  readonly execution:ExecutionViewIdentity|null;
  readonly placement:PlacementDetails|null;
  readonly exchanges:readonly ExchangeDetails[];
  readonly recovery:RecoveryDetails|null;
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
  buildNodeCard(graph:RawGraph,selection:NodeSelection,snapshot:RawSnapshot|null,execution?:ExecutionProjection|null):NodeCard;
  buildGraphModel(graph:RawGraph,options:{choice:GraphChoice;snapshot:RawSnapshot|null;selection:NodeSelection|null}):GraphModel;
  buildNodeDetails(graph:RawGraph,selection:NodeSelection):NodeDetails;
  normalizeExecutionPlan(entry:RawActionEntry,binding:GraphBinding):ExecutionProjection;
}
// C8: frontend read identity only. GraphBinding HTTP schema stays unchanged.
type ExecutionFormat = 'legacy'|'v3';
interface ExecutionViewIdentity {
  readonly project:ProjectId; readonly basisDigest:SHA256;
  readonly runId:string; readonly attempt:number;
  readonly actionId:string; readonly dagId:SHA256; readonly planId:SHA256;
  readonly planRevision:number; readonly format:ExecutionFormat;
}
type ResourceRole='cpu'|'gpu'|'remote_api'|'io';
type ExchangeMode='memory_incremental'|'durable_incremental'|'finite_sealed';
type RecoveryMode='checkpoint'|'final_commit'|'delivery_only'|'attempt_only';
interface PlacementDetails {
  readonly stageId:string; readonly role:ResourceRole|null;
  readonly profile:Readonly<Record<string,JSONValue>>|null;
  readonly partitionCount:number|null;
  readonly replicaMin:number|null; readonly replicaTarget:number|null; readonly replicaMax:number|null;
  readonly fusedOccurrences:readonly SHA256[];
  readonly basis:'sealed'; // profile/replicaTarget are not observed use/live worker count
}
interface ExchangeDetails {
  readonly channelId:string; readonly edgeId:SHA256|null;
  readonly mode:string; // legacy retains its original enum, never relabel a stored mode
  readonly routing:string|null; readonly reason:string|null;
  readonly creditRows:number|null; readonly creditBytes:number|null;
  readonly storageBytes:number|null;
  readonly basis:'sealed';
}
interface RecoveryDetails {
  readonly declared:string; // legacy original value; v3 validates RecoveryMode
  readonly limitations:readonly string[];
  readonly restriction:string|null;
  // No canResume: only the existing manager capability authorizes that action.
}
interface V3EdgeEndpoint {
  readonly logical_node_id:SHA256; // compatibility alias of semantic_id
  readonly occurrence_id:SHA256;
  readonly port:string;
}
interface V3ProjectedEdge {
  readonly edge_id:SHA256;
  readonly from:V3EdgeEndpoint; readonly to:V3EdgeEndpoint;
}
interface V3ProjectedNode {
  readonly logical_node_id:SHA256; readonly occurrence_id:SHA256;
  readonly op_kind:string; readonly input_ports:readonly string[]; readonly output_ports:readonly string[];
  readonly traits:Readonly<{state:string;effect:string;resource_role:ResourceRole;mergeable:boolean}>;
}
interface V3ProjectedStage {
  readonly stage_id:string; readonly partition_count:number;
  readonly profile:Readonly<Record<string,JSONValue>>;
  readonly replica_min:number; readonly replica_target:number; readonly replica_max:number;
  readonly resident_bytes:number; readonly output_bytes:number; readonly state_bytes:number;
}
interface V3ProjectedExchange {
  readonly channel_id:string; readonly edge_id:SHA256;
  readonly mode:ExchangeMode; readonly routing:string; readonly keys:readonly string[];
  readonly reason:string; readonly route_epoch:number; readonly read_domain:string;
  readonly credit_rows:number; readonly credit_bytes:number; readonly storage_bytes:number;
  readonly producer_count:number; readonly consumer_count:number;
}
interface V3ObservationEntry {
  readonly logical_node_id:SHA256; readonly occurrence_id:SHA256;
  readonly report_node_id:ReportNodeId; readonly report_occurrence:number;
  readonly availability:'observed'|'not_in_page';
  // This bounded summary does not contain a new metric format. Detail reads keep
  // their original rows, and must carry checked plan scope before v3 node use.
}
type V3ObservationPage =
  | Readonly<{available:false;reason:string}>
  | Readonly<{available:true;action_id:string;attempt:number;dag_id:SHA256;
      plan_id:SHA256;plan_revision:number;has_more:boolean;entries:readonly V3ObservationEntry[]}>;
interface RawV3ActionEntry {
  readonly version:'v3'; readonly action_id:string; readonly status:'sealed'; readonly binding_level:'sealed';
  readonly plan_id:SHA256;
  readonly dag:Readonly<{
    schema_version:'demiflow.dataflow_graph.v3';action_id:string;dag_id:SHA256;
    nodes:readonly V3ProjectedNode[];edges:readonly V3ProjectedEdge[];
    sources:readonly SHA256[];terminals:readonly SHA256[];
  }>;
  readonly physical:Readonly<{
    plan_id:SHA256;plan_revision:number;
    stages:Readonly<Record<SHA256,V3ProjectedStage>>;exchanges:readonly V3ProjectedExchange[];
    recovery:RecoveryMode;limitations:readonly string[];
    policy:Readonly<Record<string,JSONValue>>;
  }>;
  // The existing validated bridge and observation page are consumed independently.
  readonly identity_bridge?:Readonly<Record<string,JSONValue>>;
  readonly node_observations?:V3ObservationPage|readonly JSONValue[];
}
type RawActionEntry = RawV3ActionEntry|Readonly<Record<string,JSONValue>>;
interface ProjectedNode {
  readonly logicalNodeId:SHA256; readonly occurrenceId:SHA256;
  readonly kind:string; readonly inputPorts:readonly string[]; readonly outputPorts:readonly string[];
  readonly placement:PlacementDetails|null;
}
interface ProjectedEdge {
  readonly edgeId:SHA256|null;
  readonly from:Readonly<{logicalNodeId:SHA256;occurrenceId:SHA256|null;port:string}>;
  readonly to:Readonly<{logicalNodeId:SHA256;occurrenceId:SHA256|null;port:string}>;
}
type ProjectionReason='not_sealed'|'unsupported_plan_schema'|'identity_mismatch'|
  'v3_endpoint_identity_missing'|'invalid_topology'|'invalid_physical_binding'|'projection_budget_exceeded';
type ExecutionProjection =
  | Readonly<{available:false;reason:ProjectionReason;detail:string}>
  | Readonly<{available:true;identity:ExecutionViewIdentity;nodes:readonly ProjectedNode[];
      edges:readonly ProjectedEdge[];exchanges:readonly ExchangeDetails[];recovery:RecoveryDetails}>;
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

// Optional all-or-none extension to the existing v2 run URL (A10), not HTTP parameters.
interface RunPlanLocator {
  readonly dag_id:SHA256; readonly plan_id:SHA256;
  readonly plan_revision:number; readonly plan_format:ExecutionFormat;
}
