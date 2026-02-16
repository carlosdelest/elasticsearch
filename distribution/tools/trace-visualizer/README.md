# ES|QL Query Trace Visualizer

Interactive timeline visualization tool for Elasticsearch query traces. Displays ES|QL query execution traces as an interactive waterfall timeline, similar to Chrome DevTools Network panel.

## Quick Start

### 1. Capture Trace Data

Run an ES|QL query with tracing enabled:

```bash
# Save the entire response (recommended)
curl -X POST "localhost:9200/_query?trace=true" \
  -H 'Content-Type: application/json' \
  -d '{"query": "FROM logs | STATS count() BY status"}' \
  > trace.json

# Or extract just the trace field
curl -X POST "localhost:9200/_query?trace=true" \
  -H 'Content-Type: application/json' \
  -d '{"query": "FROM logs | STATS count() BY status"}' \
  | jq '.trace' > trace.json
```

The `trace` parameter enables request-level tracing and includes a `trace` field in the response containing the `QueryTraceResults` JSON. The visualizer accepts both the full response format or just the extracted trace data.

### 2. Open the Visualizer

**Option 1: Direct File Open**
```bash
open esql-trace-visualizer.html
```

**Option 2: Via HTTP Server**
```bash
# Python 3
python3 -m http.server 8000

# Python 2
python -m SimpleHTTPServer 8000

# Node.js (npx)
npx http-server

# Then navigate to:
# http://localhost:8000/esql-trace-visualizer.html
```

### 3. Load and Analyze

**Option A: File Upload**
1. Drag and drop your `trace.json` file onto the page, or click to browse

**Option B: Paste JSON**
1. Copy the ES|QL response JSON (with `trace` field)
2. Paste it into the text area on the page
3. Click "Load Trace" (or press Ctrl/Cmd+Enter)

**Analyze:**
1. View the waterfall timeline showing all spans
2. Click on any span to see detailed information
3. Use the expand/collapse controls to navigate deep traces
4. Search for specific operations using the search box

## Features

### Phase 1 (Current - MVP)
- ✅ Single self-contained HTML file (no build step, no dependencies)
- ✅ D3.js-powered waterfall visualization
- ✅ Drag-and-drop file upload
- ✅ Paste JSON directly into text area (with Ctrl/Cmd+Enter support)
- ✅ Accepts both full ES|QL response or direct trace format
- ✅ Hierarchical span tree with indentation
- ✅ Span selection with detailed attribute viewer
- ✅ Duration formatting (ns/μs/ms/s auto-scaling)
- ✅ Expand/collapse tree navigation
- ✅ Search functionality

### Phase 2 (Planned - Enhanced UX)
- ⏳ Zoom and pan for large traces
- ⏳ Minimap overview
- ⏳ Advanced search and filter
- ⏳ Critical path highlighting
- ⏳ Export to PNG
- ⏳ URL state persistence for sharing

### Phase 3 (Planned - Advanced Features)
- ⏳ Compare two traces side-by-side
- ⏳ Dark mode
- ⏳ Keyboard shortcuts
- ⏳ Performance statistics panel
- ⏳ Flame graph view option

## Trace Data Format

The visualizer accepts both full ES|QL response format or direct trace data:

**Full Response Format (recommended):**
```json
{
  "columns": [...],
  "values": [...],
  "trace": {
    "trace_id": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6",
    "total_duration_nanos": 150000000,
    "spans": [...]
  }
}
```

**Direct Trace Format:**
```json
{
  "trace_id": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6",
  "total_duration_nanos": 150000000,
  "spans": [
    {
      "span_id": "a1b2c3d4e5f6a7b8",
      "operation_name": "esql.query",
      "start_time_nanos": 1704067200000000000,
      "end_time_nanos": 1704067200150000000,
      "duration_nanos": 150000000,
      "attributes": {
        "query": "FROM logs | LIMIT 10"
      },
      "children": [
        {
          "span_id": "b2c3d4e5f6a7b8c9",
          "operation_name": "esql.parse",
          "start_time_nanos": 1704067200000000000,
          "end_time_nanos": 1704067200008000000,
          "duration_nanos": 8000000,
          "attributes": {},
          "children": []
        }
      ]
    }
  ]
}
```

### Required Fields

- `trace_id` (string): W3C Trace Context format trace ID (32 hex characters)
- `total_duration_nanos` (number): Total trace duration in nanoseconds
- `spans` (array): Array of root span objects

### Span Fields

- `span_id` (string): Unique span identifier (16 hex characters)
- `operation_name` (string): Name of the operation (e.g., "esql.parse", "esql.execute")
- `start_time_nanos` (number): Start time in nanoseconds since epoch
- `end_time_nanos` (number): End time in nanoseconds since epoch
- `duration_nanos` (number): Duration in nanoseconds
- `attributes` (object): Key-value pairs with additional context
- `children` (array): Nested child spans

## Visualization

The waterfall timeline shows:
- **Time axis**: Scaled in appropriate units (ns, μs, ms, s)
- **Span bars**: Horizontal bars positioned by start time and sized by duration
- **Hierarchy**: Indented tree structure with expand/collapse controls
- **Selection**: Click any span to view detailed timing and attributes
- **Duration labels**: Displayed next to each span bar

## Browser Support

Works in all modern browsers:
- Chrome/Edge (Chromium) 90+
- Firefox 88+
- Safari 14+

Requires JavaScript enabled.

## Troubleshooting

### "Invalid trace format" error
- Ensure your JSON contains `trace_id` and `spans` fields
- Check that the JSON is valid (use `jq` or `python -m json.tool` to validate)
- Verify you're using the correct trace output format from ES|QL

### Spans not rendering
- Check browser console for JavaScript errors
- Verify span objects have required fields: `span_id`, `operation_name`, `start_time_nanos`, `end_time_nanos`
- Ensure duration values are positive numbers

### Performance issues with large traces
- The current implementation supports up to ~500 spans efficiently
- For larger traces, Phase 2 will add virtual scrolling and canvas rendering
- Consider filtering your trace or analyzing specific subtrees

## Examples

Example trace files are provided in the `examples/` directory:

- `simple-query-trace.json`: Basic single-shard query
- `complex-trace.json`: Multi-shard query with deep nesting

## Architecture

This is a standalone HTML tool with:
- **Zero dependencies**: Just open the HTML file in a browser
- **Offline capable**: All code is embedded (except D3.js from CDN)
- **No build step**: No npm, no webpack, no compilation
- **Portable**: Copy the HTML file anywhere

The tool uses:
- **D3.js v7** for SVG-based timeline rendering
- **Vanilla JavaScript (ES6+)** for all logic
- **CSS Grid/Flexbox** for responsive layout

## Development

To modify the visualizer:

1. Edit `esql-trace-visualizer.html` directly
2. Refresh the page in your browser to see changes
3. Use browser DevTools for debugging

No build step required!

## License

Copyright Elasticsearch B.V. Licensed under the Elastic License 2.0, GNU Affero General Public License v3.0 only, and Server Side Public License v1.

## Related Documentation

- [ES|QL Query API](../../docs/reference/esql/esql-query-api.asciidoc)
- [Telemetry Tracing](../../server/src/main/java/org/elasticsearch/telemetry/tracing/)
- [QueryTraceResults](../../server/src/main/java/org/elasticsearch/telemetry/tracing/QueryTraceResults.java)
