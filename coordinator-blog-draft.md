# The Coordinator: orchestrating complex and disaggregated inference without a sidecar

## TLDR

llm-d-router disaggregates prefill and decode (and, for multimodal input, encoding)
through a sidecar container that runs on the decode pod. The Coordinator is an
alternative that runs the same disaggregation from a standalone service in front of the
Inference Gateway, with no pod-local sidecar at all. Both go through the same Gateway,
the same Endpoint Picker (EPP), and the same GAIE `ext_proc` protocol; what changes is
*where* request orchestration is driven from, and *when* each pod gets
picked: one EPP call per phase, made right before that phase runs, instead of one
scheduling cycle picking every pod up front.

The most concrete payoff for a deployer: the pipeline is composable. A new processing
stage is a small, self-contained Go type registered under a name and referenced from
YAML; no need to understand the sidecar's KV-connector protocols or its central
dispatch logic to add one. That buys no sidecar to deploy, version, or upgrade per
decode pod, plus a foundation the sidecar's fixed cascade can't offer: branching/looping
steps, cross-phase cancellation, and reuse of the same selection machinery for model or
endpoint choice, not just pods. The cost is more Gateway/EPP round trips per request
than the sidecar's single scheduling cycle; section 3 quantifies that once benchmark
numbers are in.

## 1. Why a sidecar, and why not a sidecar

The Gateway API Inference Extension's
[endpoint-picker protocol](https://github.com/kubernetes-sigs/gateway-api-inference-extension/tree/main/docs/proposals/004-endpoint-picker-protocol),
carried over Envoy's
[external processing filter](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/ext_proc_filter)
(`ext_proc`), is
structurally one request in, one decision out: a Gateway (such as  Envoy) calls
the EPP once per proxied request, the EPP returns a decision. The  data plane is
managed by the Gateway and the EPP acts as a control plane. The EPP cannot itself
issue a second downstream call; it has no way to pick a prefill pod, wait for a
response, and then pick a decode pod within that same cycle. Disaggregating prefill from
decode needs exactly that: two hops instead of one. Adding encode disaggregation makes
it three, and the encode hop is not a single call: it fans out one request per multimodal
entry in the prompt, each needing its own endpoint decision, and all of them have to
complete before prefill and decode can run.

llm-d-router's sidecar exists to do what `ext_proc` is not meant to. The EPP still runs
one scheduling cycle, but instead of picking one pod it picks every pod the request
needs: decode always, one encode pod per multimodal entry if the request has multimodal
content, and prefill if a decider judges it worthwhile. All of those choices are made up
front, in a fixed order, before decode is even contacted. The EPP writes them onto
request headers (`x-prefiller-host-port`, `x-encoder-hosts-ports`) and the Gateway
forwards to the chosen decode pod. A sidecar running alongside that pod reads the
headers and drives the rest itself: dispatch to encode workers, send prefill a
`max_tokens=1` request, collect the returned KV-transfer parameters, run decode locally
against the transferred KV cache, and stream the response back to the client. No sidecar
or coordination logic runs on the prefill or encode pods; only decode carries this cost,
and it carries all of it.

This is a reasonable design for a fixed, small phase count, and it's what shipped. But
header-chaining doesn't scale well as the number of orchestration concerns grows. Two
pressures compound:

- **Every new concern needs its own header contract.** The sidecar's own codebase shows
  the effect: a central `Server` struct has accumulated responsibilities across three KV
  connector protocols (`nixlv2`, `sglang`, `shared-storage`) plus encode and data-parallel
  add-ons, each with its own dispatch boilerplate, and a single request handler's
  decision tree grows a new branch for each header it has to understand
  (`X-Prefill-Host`, `X-Encoder-Endpoints`, `X-Data-Parallel-Host-Port`, and so on). This
  isn't a hypothetical scaling concern; it's the shape the code has already taken.
- **The EPP has to commit to every pod choice in one cycle, including decode, long
  before decode is actually contacted.** There's no room in that model for a decision
  that depends on what an earlier phase returned.

The Coordinator replaces both pressures with one mechanism: a per-phase `EPP-Profile`
call, made right before that phase runs, so pod selection for decode defers to the
moment decode is actually invoked rather than being locked in earlier in the request's
life. A new orchestration concern becomes a new pipeline step, not a new header.

```
A. SIDECAR: one cycle + headers   B. COORDINATOR: one call per phase
-------------------------------   ----------------------------------

       Client                            Client
         |                                 |
         v                                 v
  Inference Gateway                 Inference Gateway
         |                                 |
         |  ext_proc: ONE cycle            |  no EPP-Profile header:
         v                                 |  default route
        EPP                                v
  picks decode, encode(s) and       Coordinator Service --> side services
  prefill ALL up front, in a        (step pipeline)         (media download,
  fixed order                              |                 render / tokenize)
         |                                 |
         |  decisions written              |  one call per phase, tagged
         |  onto request headers:          |  EPP-Profile: encode, prefill
         |   x-encoder-hosts-ports         |  or decode (re-enters the
         |   x-prefiller-host-port         |  same Gateway)
         v                                 v
 +----------------------------+     Inference Gateway <-- ext_proc --> EPP
 | Decode pod                 |            |
 |  SIDECAR reads the headers |            |  one scheduling profile per
 |  and drives the rest:      |            |  phase; the decode pod is
 |   -> encode workers        |            |  picked at the moment decode
 |   -> prefill, max_tokens=1 |            |  is invoked
 |   -> local decode on the   |            v
 |      transferred KV cache  |     Encode / Prefill / Decode vLLM pods
 +----------------------------+     (one InferencePool, no sidecar)

  orchestration lives on the        orchestration lives in front of the
  decode pod; a new concern         Inference Gateway; a new concern
  means a new header contract       means a new pipeline step; the other
  plus new dispatch logic           step internals never have to be
  inside the sidecar itself         touched or understood
```

*(Panel B adapted from
[`docs/coordinator_architecture.md`](https://github.com/llm-d/llm-d-router/blob/main/docs/coordinator_architecture.md);
panel A from
[`docs/disaggregation.md`](https://github.com/llm-d/llm-d-router/blob/main/docs/disaggregation.md).
Both panels show the full encode/prefill/decode cascade; the sidecar also supports P/D
and E/PD variants, and the Coordinator can skip the cascade entirely via its
`conditional-decode` step, which tries decode first and falls back on a 412.)*

*This framing hasn't been checked against the sidecar maintainers' own account yet;
their context on why headers were the right call at the time is worth including for
fairness before this goes out.*

## 2. Design goals

The Coordinator's design serves six goals, each traceable directly to an architectural
choice:

| Goal | Architecture |
|---|---|
| Composable steps, easy extension | `Step` interface + registry; pipeline is config, not code; a new stage is a type name and a factory, added without touching the pipeline or any existing step |
| Deferred, per-phase selection | One EPP call per phase (`EPP-Profile` header) instead of one cycle picking every pod up front |
| Stateless | Per-request `RequestContext`; any replica serves any request; scales behind a plain load balancer, no coordination between instances |
| Tokens-in / tokens-out operation | Steps exchange token IDs on `RequestContext` instead of raw text, which keeps an RL training loop in token space with no detokenize/re-tokenize round-trips |
| One tokenization per request | `render` tokenizes once; encode and prefill then call the token-array endpoint `/inference/v1/generate` with those IDs (`use_openai_format: false`) instead of `/v1/chat/completions`, so no worker re-tokenizes. Experimental; the OpenAI-format path is the tested default |
| No sidecar dependency | Orchestration moves off the decode pod into a standalone service in front of the Inference Gateway |

### Goal by goal

**Composable steps, easy extension.** The pipeline is an ordered list of named steps
resolved from YAML at startup, and the executor runs them top to bottom without knowing
what any of them does. The built-ins are themselves just steps registered this way:
`async-broker`, `replace-media-urls`, `render`, `conditional-decode`, `encode`,
`prefill`, `decode`. Nothing about that list is privileged, which is what makes adding
to it cheap; the next subsection walks through what writing one actually involves.

**Deferred, per-phase selection.** Because every phase is its own call back through the
Gateway, every phase gets its own `ext_proc` cycle and its own scheduling profile,
chosen by the `EPP-Profile` header and narrowed to the right role by a filter
(`encode-filter`, `prefill-filter`, `decode-filter`) inside a single EPP. The practical
consequence is that a later decision can depend on what an earlier phase returned, which
the one-cycle model structurally cannot express. The `conditional-decode` step is the
clearest example: it optimistically calls decode first, and only if that pod answers
`412 Precondition Failed` does the coordinator fall through to the full encode/prefill
cascade. Under the header model that choice would have to be guessed before decode was
ever contacted.

**Stateless.** Everything the pipeline needs for a request lives on a per-request
`RequestContext`, including the token IDs from `render`, the `ECTransferParams` handed
from encode to prefill, and the `KVTransferParams` handed from prefill to decode. No
state outlives the request and no replica knows anything another replica needs, so the
Coordinator deploys as a plain Deployment behind a Service: scale it out, roll it,
restart it, with no leader election, no sharding, and no sticky routing. The operational
story is the same as for any stateless proxy; in-flight requests on a dying pod fail,
and everything else is unaffected.

**Tokens-in / tokens-out operation.** Steps pass token IDs to each other rather than
re-serialized text. For an RL training loop that matters more than it sounds: a
detokenize/re-tokenize round-trip between phases can land on different token boundaries
than the ones the policy actually sampled, so the trajectory you train on stops matching
the trajectory you generated. Keeping the whole path in token space removes that class
of mismatch entirely instead of papering over it.

**One tokenization per request.** `render` tokenizes the prompt once, and encode and
prefill then post those IDs to the token-array endpoint `/inference/v1/generate` rather
than `/v1/chat/completions`, so no worker re-does the work. This is gated on
`use_openai_format: false`, and it is still experimental; the OpenAI-format path remains
the tested default, and decode always forwards on the client's original path regardless
of the setting. Treat the saving as a real but not-yet-default optimization.

**No sidecar dependency.** Nothing extra runs on the decode pod, so there is no sidecar
image to build, version, or upgrade in lockstep with vLLM, and no per-pod container to
co-schedule and debug. What replaces it is one standalone service plus a default Gateway
route, which is a single thing to run for the whole cluster instead of one per decode
replica. That is a straight trade, not a free win: the Coordinator is a new component in
the request path, and section 3 accounts for what that costs.

### Writing a step

The first goal is the one to sit with, because it's the most concrete win for whoever
has to extend this thing. A step implements two methods:

```go
type Step interface {
    Name() string
    Execute(ctx context.Context, reqCtx *RequestContext) error
}
```

It's constructed by a factory, registered under a type name in an `init()`, and wired
into the pipeline entirely through YAML:

```yaml
pipeline:
  steps:
    - type: render
      params:
        address: "http://rendering-service:8080"
    - type: my-new-step
      params:
        threshold: 16
    - type: decode
```

The pipeline executor calls steps by name and knows nothing about what's inside any of
them. Compare that to extending the sidecar: doing so means understanding the specific
KV-connector protocol code and the central `Server` struct's dispatch logic described in
section 1, implementation detail a step author for the Coordinator never has to touch.
That's the difference between writing one small, isolated Go type plus a YAML entry, and
learning a sidecar's internals to add a capability. It's also the reason a new step
doesn't require a new header: state a step needs from an earlier phase lives on
`RequestContext`, not on the wire.

## 3. What it costs, what it buys

The concrete request flow is: client → Gateway → Coordinator → (Gateway → EPP → pod) ×
N phases. Said plainly, so it doesn't get lost in the payoff argument: this is *more*
Gateway/EPP round trips per request than the sidecar's single scheduling cycle, through
the same Inference Gateway. The Coordinator is an active client of the Gateway, issuing
and consuming several requests in sequence (and, for multimodal fan-out, in parallel)
rather than being handed one already-resolved cascade.

Some of that overhead is absorbed rather than paid outright:

- Multimodal encode fan-out already runs in parallel: one Gateway call per multimodal
  entry, concurrently, not serialized per item.
- The render step tokenizes the prompt once and reuses the token IDs across encode,
  prefill, and decode on the tokens-in (`/inference/v1/generate`) path, so those workers
  never re-tokenize. The OpenAI-format path re-tokenizes on every worker regardless of
  which orchestration model is in front of it, so this saving is specific to the
  tokens-in path, not a blanket win.
- `conditional-decode` tries decode alone first, optimistically: if the chosen decode pod
  already holds what it needs, it serves the request directly and the rest of the
  pipeline (encode, prefill) never runs at all.

The net argument is that this bounded overhead is paid back by not deploying,
versioning, and upgrading a sidecar per decode pod, plus the composability payoff from
section 2. Whether "bounded" holds up under load is an empirical claim, not a
rhetorical one, so:

**Benchmarks (pending; no numbers below are real yet):**

- Per-hop added latency, p50/p99, Coordinator vs. sidecar, same encode/prefill/decode
  topology and worker pool sizes.
- Throughput at fixed concurrency for both models, to check the extra round trips don't
  show up as a capacity cliff rather than a fixed tax.
- Tokens-in vs. OpenAI-format wall-clock effect on the re-tokenization path, to quantify
  the tokenize-once saving described above. (The *size* half of that tradeoff, request
  bodies growing from kilobytes to tens of megabytes as image resolution increases on
  the tokens-in path, is already measured in `docs/coordinator_architecture.md`'s
  "Format tradeoff" section; what's missing is the time cost of re-tokenization itself.)

Run these against the existing benchmark in the
[coord-disaggregation guide](https://github.com/llm-d/llm-d/tree/main/guides/coord-disaggregation#benchmark)
rather than standing up a separate load-test harness; that guide's benchmark already
covers this topology.

*TODO: replace this section's bullets with real p50/p99 and throughput numbers once the
benchmark runs land. Do not publish with placeholder or estimated figures.*

## 4. What this unlocks beyond disaggregation

Section 2's modularity point is the headline win available today. Everything below is
what the same pipeline model opens up next, each grounded in an existing design
document, none of it shipped yet.

**Cycles and conditionals in the pipeline.** Today's pipeline is a flat, ordered list:
every step runs exactly once, top to bottom. Agentic loops and tool-calling need two
things that model doesn't have: a conditional (branch to a guardrail step; only run
the tool-call step if the model's response actually requests a tool) and a cycle (repeat
a sub-sequence of model call, tool call, model call until the model returns a final
answer, or retry-with-backoff around a flaky call-out, bounded by a hard iteration cap so
a runaway loop can't become a liveness problem). This is a change to the pipeline
executor and the `Step` contract itself, not just new step implementations to slot in.

**Cancellation today; migration as a roadmap item, not a shipped feature.**
Cross-phase state already lives on `RequestContext` instead of being scattered across
sidecar memory and request headers, which makes context cancellation naturally a
pipeline-wide concern rather than something each hop has to handle separately. At the
EPP's own queue layer, cancellation is already one of the three ways a queued request
unblocks. What is *not* implemented anywhere in the stack today is request migration:
re-routing a request to a sibling pod with a warm prefix cache before evicting it, rather
than just evicting it. The architecture removes an obstacle to building this (state
that would need to move is centralized, not sidecar-local), but it doesn't build it.

**The same filter/score/pick machinery, generalized past pods.** Strip away the "pod"
framing and an EPP is a generic component: given candidate items and per-item data,
filter down to eligible candidates, score them, pick one. Nothing about that flow
requires the items to be pods; the same shape works for picking a model before any pod
decision is made, or for picking a remote endpoint/cluster (which might itself aggregate
local pod capacity, or front an external API). Each of these is its own single-purpose
EPP deployment (a model-selection hub, then an endpoint-selection EPP, then an ordinary
pod-selection EPP downstream), chained together rather than one EPP instance learning to
reason about several kinds of item at once. The Coordinator generalizes the same way:
strip away "encode/prefill/decode" and it's a stateless step-orchestrator that could just
as well call out to guardrail services and tool endpoints for an agentic-loop
deployment, using the same executor and the same `RequestContext` it uses for
disaggregated inference today.

**WebSocket sessions: an idea, not a plan.** `ext_proc` makes one decision per HTTP
request. A WebSocket connection is a single HTTP request that upgrades and then carries
an open-ended number of frames outside that request/response model, so `ext_proc` has no
per-frame decision point once the upgrade completes. A standalone Coordinator sitting in
front of the connection, rather than a one-shot `ext_proc` call, looks like a more
natural place to route or re-route a long-lived session (a streaming agent conversation,
live tool use) than trying to make the EPP reason about it. This point has no design
doc or implementation behind it in this repository yet, and it hasn't been checked
against EPP maintainers, who may know of a per-frame decision mechanism this outline is
assuming doesn't exist. Treat it as a forward-looking idea until that's confirmed.

## 5. Closing

The Coordinator is one instantiation of a more general pattern: pull orchestration out
of wherever it's structurally forced to live (a sidecar, a fixed EPP scheduling cycle)
and into a configurable pipeline of steps against a stable per-request context. For the
full mechanics (the `RequestContext` field reference, the EPP integration details, the
connector protocols, and the step-authoring guide), see
[docs/coordinator_architecture.md](docs/coordinator_architecture.md). For where the
pipeline model is headed next, see the
[composable EPP/Coordinator proposal](thoughts/shared/plans/proposal-composable-epp-coordinator.md).

---

## Outstanding before this ships

- [ ] Confirm section 1's sidecar framing with sidecar maintainers.
- [ ] Run the benchmarks in section 3 against the coord-disaggregation guide's harness;
      replace the pending-numbers note with real p50/p99 and throughput figures.
- [ ] Verify with EPP maintainers that per-frame `ext_proc` decisions genuinely don't
      exist, before the WebSocket point in section 4 goes out unchanged.
- [ ] Decide target venue/audience (llm-d blog vs. Anthropic-adjacent vs. CNCF); affects
      how much EPP/`ext_proc` background needs spelling out for readers unfamiliar with
      the project.
