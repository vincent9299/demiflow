"""Arrow result components for Dataset.search_web/fetch_documents/read_documents.

Consumers compose business stage schemas around these components. No business
question type, review verdict, round number or adoption schema is defined here.
"""
import pyarrow as pa
STRINGS=pa.list_(pa.string())
OBJECT_REF=pa.struct([('uri',pa.string()),('sha256',pa.string())])
HTTP_RECEIPT=pa.struct([('host',pa.string()),('method',pa.string()),('http_status',pa.int64()),
                        ('bytes',pa.int64()),('elapsed_s',pa.float64()),('retry_after',pa.string())])
ATTEMPT=pa.struct([('attempt',pa.int64()),('status',pa.string()),('http_status',pa.int64()),
                   ('reason',pa.string()),('elapsed_s',pa.float64()),('http',pa.list_(HTTP_RECEIPT)),
                   ('route_kind',pa.string()),('retry_after_s',pa.float64())])
CANDIDATE=pa.struct([('url',pa.string()),('title',pa.string()),('snippet',pa.string()),
                     ('engines',STRINGS),('result_kind',pa.string()),
                     ('img_src',pa.string()),('thumbnail_src',pa.string()),
                     ('resolution',pa.string()),('declared_width',pa.int64()),('declared_height',pa.int64()),
                     ('declared_file_bytes',pa.int64()),('mime_type',pa.string())])
SEARCH_CAPABILITIES=pa.struct([('paging',pa.bool_()),('max_page',pa.int64()),
    ('time_range_support',pa.bool_()),('safesearch',pa.bool_()),('language_support',pa.bool_()),
    ('language',pa.string()),('engine_type',pa.string())])
ENGINE_RECEIPT=pa.struct([('engine',pa.string()),('status',pa.string()),('reason',pa.string()),
    ('backend',pa.string()),('parser_version',pa.string()),('response_sha256',pa.string()),('browser_metrics_json',pa.string()),
    ('attempts',pa.list_(ATTEMPT)),('receipt_id',pa.string()),('retry_after_s',pa.float64()),('resume_at',pa.float64()),
    ('capabilities',SEARCH_CAPABILITIES),('ignored_parameters',STRINGS)])
SEARCH_RESULT=pa.struct([('status',pa.string()),('reason',pa.string()),
                        ('candidates',pa.list_(CANDIDATE)),('attempts',pa.list_(ATTEMPT)),
                        ('engine_receipts',pa.list_(ENGINE_RECEIPT)),('response_json',pa.string()),
                        ('runtime',pa.string()),('profile',pa.string()),('parameters_json',pa.string())])
ACQUISITION=pa.struct([('kind',pa.string()),('origin',pa.string()),('source_id',pa.string()),
                       ('revision',pa.string()),('registered_at',pa.string())])
DOCUMENT_RESULT=pa.struct([('url',pa.string()),('final_url',pa.string()),('content_type',pa.string()),
    ('retrieved_at',pa.string()),('document_ref',OBJECT_REF),('raw_ref',OBJECT_REF),
    ('status',pa.string()),('reason',pa.string()),('attempts',pa.list_(ATTEMPT)),('acquisition',ACQUISITION)])
SELECTED_BLOCK=pa.struct([('evidence_id',pa.string()),('document_ref',OBJECT_REF),('block_id',pa.string())])
HEADING=pa.struct([('block_id',pa.string()),('text',pa.string())])
READING=pa.struct([('document_ref',OBJECT_REF),('url',pa.string()),('title',pa.string()),
    ('block_count',pa.int64()),('selected_block_ids',STRINGS),('unread_ranges',STRINGS),
    ('headings',pa.list_(HEADING)),('heading_total',pa.int64()),('status',pa.string()),('reason',pa.string())])
REQUEST_RESULT=pa.struct([('request_id',pa.string()),('status',pa.string()),('reason',pa.string())])
IMAGE_RESULT=pa.struct([
    ('url',pa.string()),('final_url',pa.string()),('status',pa.string()),('reason',pa.string()),
    ('image_ref',OBJECT_REF),('content_type',pa.string()),('format',pa.string()),
    ('width',pa.int64()),('height',pa.int64()),('size_bytes',pa.int64()),('filter_stage',pa.string()),
    ('retrieved_at',pa.string()),('origin',pa.string()),('attempts',pa.list_(ATTEMPT))])
