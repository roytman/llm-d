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
than the sidecar's single scheduling cycle. Section 5 quantifies it: a few percent on
TTFT for a single request, nothing measurable in end-to-end latency, and under bursty
heterogeneous multimodal load the deferred decode pick turns into 4-10% *higher*
throughput than the sidecar.

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
the one-cycle model structurally cannot express. Section 5 shows this paying off as
throughput under bursty load, where picking decode after prefill keeps the decode pods
balanced. The `conditional-decode` step, covered in section 4, is the clearest
example: it calls decode first and only runs encode and
prefill if that pod turns the request down. Under the header model that choice would
have to be guessed before decode was ever contacted.

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

**One tokenization per request.** `render` tokenizes the prompt once and every later
step works from those IDs, so neither the EPP nor the vLLM engine has to redo the work
per phase. This one is experimental and is worth its own treatment; section 4 covers
what it replaces and what it is gated on.

**No sidecar dependency.** Nothing extra runs on the decode pod, so there is no sidecar
image to build, version, or upgrade in lockstep with vLLM, and no per-pod container to
co-schedule and debug. What replaces it is one standalone service plus a default Gateway
route, which is a single thing to run for the whole cluster instead of one per decode
replica. That is a straight trade, not a free win: the Coordinator is a new component in
the request path, and section 5 accounts for what that costs.

## 3. Writing a new step

The first of the design goals, composability, is the one to sit with, because it's the most
concrete win for whoever has to extend this thing. A Coordinator step implements two methods:

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

## 4. Coordinator optimizations

Two of the design goals above are not just properties of the architecture but active
optimizations the Coordinator performs on the request. Both exist because the pipeline
sits in front of the Gateway and can see the whole request before any phase runs.

### Conditional decode: skipping encode and prefill entirely

The full cascade is not always necessary. If the decode pod already holds the KV cache
for this prompt, or the prompt is short enough that remote prefill would cost more than
it saves, then encoding and prefilling are pure waste. The problem is that the
Coordinator cannot know either of those things by itself; only the EPP, which tracks
per-pod cache state, can.

`conditional-decode` is an optional step that asks instead of guessing. It sends the
user's request to the decode profile carrying the HTTP header `Prefer: if-available`,
using the `Prefer` mechanism from
[RFC 7240](https://www.rfc-editor.org/rfc/rfc7240) to express a preference the server is
free to decline. Semantically that is exactly the question being asked: serve this now
if you can, or tell me if you cannot.

Two outcomes follow:

- **The EPP can satisfy it.** A decode pod has the required KV cache, or the prompt is
  below the configured threshold. The EPP forwards to the chosen pod, the worker's
  response is returned to the client as-is, and the pipeline stops there. Encode and
  prefill never execute.
- **The EPP cannot.** It answers `412 Precondition Failed`. The Coordinator treats that
  as information rather than an error, swallows it, and runs every step before decode
  normally: media fetch, render, encode per multimodal entry, prefill, then decode.

The win is the skipped work on the hit path, which for multi-turn chat is the common
case. The cost is one extra round trip to decode when the answer comes back 412, so the
step pays off in proportion to how often the optimistic guess lands. It is optional for
exactly that reason: workloads with no prefix reuse should leave it out.

The sidecar model reaches for the same saving by a different route: its
`disagg-profile-handler` runs deciders (`prefix-based-pd-decider` and friends) that gate
whether prefill and encode are requested at all, so a high cache hit or a very short
prompt skips them there too. The difference is who answers the question and when. The
decider predicts the outcome up front, inside the single scheduling cycle, from what the
EPP knows at that moment; `conditional-decode` defers it, poses it to the serving side
as a request that may be declined, and takes the `412` as the answer.

### Preventing redundant tokenization

Tokenization happens more often than the phase count suggests. Depending on
configuration, the encode and prefill EPPs tokenize the prompt themselves in order to
pick a pod, since prefix-cache-aware scoring needs token IDs rather than text. Then the
request reaches the vLLM engine and the same prompt is tokenized a second time. Multiply
that by the number of phases and a single request can be tokenized several times over,
all of it producing the identical token array.

The Coordinator's experimental answer is to tokenize once, in the `render` step, and
then make sure every downstream call carries tokens rather than text:

- For the `encode` and `prefill` steps, `chat/completions` and `responses` calls are
  replaced by `inference/v1/generate`, a token-array endpoint, so the engine receives
  the IDs directly. Those two phases return transfer metadata rather than user-visible
  text, so nothing is lost by stepping off the OpenAI surface for them.
- `completions` calls need no endpoint substitution, because `/v1/completions` already
  accepts `prompt` as an array of token IDs. The Coordinator simply replaces the text
  prompt with the tokens.

Decode is the exception, and not by choice. `inference/v1/generate` cannot yet return
text output, which is precisely what decode has to stream back to the client, so decode
always forwards on the client's original path regardless of the setting. Text output
from `inference/v1/generate` is work in progress in vLLM; once that lands, the
Coordinator can take the same shortcut for the decode step and the request stays in
token space from the Gateway all the way to the final phase.

With tokens on the wire, the EPP scores against them without re-tokenizing and the
engine has nothing left to tokenize. The whole mechanism is still experimental and gated
on `use_openai_format: false`; the OpenAI-format path remains the tested default.

## 5. What it costs, and where it wins

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
sections 2 and 3. Whether "bounded" holds up under load is an empirical claim, not a
rhetorical one, so:

### Benchmarks

The
[coord-disaggregation guide](https://github.com/llm-d/llm-d/tree/main/guides/coord-disaggregation#benchmark)
measures exactly this. To isolate the hop count rather than comparing two different
decisions about whether to disaggregate at all, both sides were forced down the full
prefill → decode path: the sidecar ran with `always-disagg-pd-decider`, and the
Coordinator ran a PD-only pipeline with no `conditional-decode` step, so its optimistic
fast path never fired. What's left between them is the Coordinator's extra Gateway/EPP
round trip per phase.

**Single request, `openai/gpt-oss-120b` on H200s** (decode 1×TP4, prefill 1×TP1, 5 GPUs,
identical on both sides, prefill and decode pinned to the same nodes to keep node
variance out of it):

- Varying input length (1 to 1,000 prompt tokens, 20-token output): median TTFT is
  1.8-4.8% higher with the Coordinator, for example 40.36ms vs. 38.51ms at 10 tokens.
  Median request latency is within 0.2-1.5% and median ITL within ±0.7%, which is
  measurement noise.

<p float="left">
  <img src="https://raw.githubusercontent.com/llm-d/llm-d/main/guides/coord-disaggregation/benchmark-results/inconcurrent_var_prompt_always_disaggr_pinned/ttft_distribution.png" alt="Varying input length: TTFT distribution" width="45%" />
  <img src="https://raw.githubusercontent.com/llm-d/llm-d/main/guides/coord-disaggregation/benchmark-results/inconcurrent_var_prompt_always_disaggr_pinned/request_latency_distribution.png" alt="Varying input length: request latency distribution" width="45%" />
</p>

- Varying output length (100 to 2,500 output tokens, 250-token input): median TTFT
  1.6-4.9% higher, median request latency within ±0.9%, median ITL within ±0.35%.

<p float="left">
  <img src="https://raw.githubusercontent.com/llm-d/llm-d/main/guides/coord-disaggregation/benchmark-results/inconcurrent_var_output_always_disaggr_pinned/ttft_distribution.png" alt="Varying output length: TTFT distribution" width="45%" />
  <img src="https://raw.githubusercontent.com/llm-d/llm-d/main/guides/coord-disaggregation/benchmark-results/inconcurrent_var_output_always_disaggr_pinned/request_latency_distribution.png" alt="Varying output length: request latency distribution" width="45%" />
</p>


**Concurrent load, `deepseek-ai/DeepSeek-V2` on H200s** (decode 6×TP4, prefill 4×TP8, 56
GPUs, `inference-perf`'s `random_spikes` scenario across nine concurrency stages from 50
to 500, 100% success on both sides):

- At 50 concurrent requests the Coordinator is *ahead*: about 10% lower median request
  latency and about 12% lower median TTFT. At 100 concurrent it's about 7% and 10%
  lower.
- From 150 up the two converge to within about 2-7% either way, with neither holding a
  consistent edge, and both scale the same as load rises.

<p float="left">
  <img src="https://raw.githubusercontent.com/llm-d/llm-d/main/guides/coord-disaggregation/benchmark-results/6Dx4GPU_4Px8GPU_DeepSeek-V2_concurrent/ttft_distribution.png" alt="Concurrent stress spikes: TTFT distribution" width="45%" />
  <img src="https://raw.githubusercontent.com/llm-d/llm-d/main/guides/coord-disaggregation/benchmark-results/6Dx4GPU_4Px8GPU_DeepSeek-V2_concurrent/request_latency_distribution.png" alt="Concurrent stress spikes: request latency distribution" width="45%" />
</p>


So the extra hop is real and it is visible, but only in TTFT, as a consistent
few-percent, single-digit-millisecond gap on single requests. It does not show up in ITL
or end-to-end request latency, and under concurrent load it does not show up at all. The
"bounded overhead" claim holds up.

### Where deferred decoding wins

Everything above answers one question: what does the extra hop cost? A further
benchmark, currently in review as
[llm-d#2658](https://github.com/llm-d/llm-d/pull/2658), asks the other one: is there a
benefit to picking the decode pod *later*? The sidecar picks prefill and decode together
before prefill starts; the Coordinator picks decode only once prefill has finished, by
which point the pool's state may have moved.

The workload is built to make that difference matter: `Qwen/Qwen3-VL-235B-A22B-Instruct`
on 4 × H200 nodes (TP=8, 2 prefill + 2 decode pods, NIXL over GKE multi-network RDMA),
heavy-tailed per-request load (log-normal output lengths, mean 2,500 and capped at
11,000 tokens, plus 1-14 mixed 360p/1080p images), released in bursts of 300 to 420 so
both decode pods sit at 85-100% KV utilization. Both arms run behind the same Gateway
with the same EPP image and scheduling profiles, and both do prefill then decode on
every request. Means of two repeats, sidecar / Coordinator:

| Burst size | Throughput (req/s) | Mean E2E (s) | Mean TTFT (s) | P99 TTFT (s) | Mean TPOT (ms) |
|---|---|---|---|---|---|
| 300 | 0.93 / **1.03** | 137 / **124** | 45.9 / **41.8** | 92 / **90** | 44.0 / 44.6 |
| 360 | 1.01 / **1.06** | 150 / **143** | 55.0 / **52.6** | 115 / **106** | 46.1 / 46.6 |
| 420 | 1.05 / **1.15** | 171 / **154** | 64.1 / **61.1** | 147 / **124** | 52.1 / **45.7** |

<p float="left">
  <img src="https://raw.githubusercontent.com/llm-d/llm-d/2b68d2f93f1d9274a4a8d9b5268d11c0497cfe68/guides/coord-disaggregation/benchmark-results/2Px8GPU_2Dx8GPU_Qwen3-VL-235B-A22B_burst/2p2d_throughput.png" alt="2P/2D burst: throughput" width="45%" />
  <img src="https://raw.githubusercontent.com/llm-d/llm-d/2b68d2f93f1d9274a4a8d9b5268d11c0497cfe68/guides/coord-disaggregation/benchmark-results/2Px8GPU_2Dx8GPU_Qwen3-VL-235B-A22B_burst/2p2d_latency.png" alt="2P/2D burst: latency" width="45%" />
</p>
<p float="left">
  <img src="https://raw.githubusercontent.com/llm-d/llm-d/2b68d2f93f1d9274a4a8d9b5268d11c0497cfe68/guides/coord-disaggregation/benchmark-results/2Px8GPU_2Dx8GPU_Qwen3-VL-235B-A22B_burst/2p2d_decode_imbalance.png" alt="2P/2D burst: per-pod decode imbalance" width="60%" />
</p>

Throughput is 4-10% higher and mean E2E latency 5-10% lower at every burst size. At 420
requests, p99 TTFT drops 15% and mean TPOT 12%. The per-pod traces show the mechanism
rather than just the outcome: under the sidecar the two decode pods start level and
drift apart as short requests finish, because every placement was fixed at t=0, until
one pod exhausts its KV with 84 requests queued behind it while the other drains. Under
the Coordinator they stay within about 15 in-flight requests of each other for the whole
burst.

This is the "deferred, per-phase selection" goal from section 2 showing up as
throughput, and it is the one place where the architecture does more than break even.
It is also narrow, and the guide is explicit about the conditions: decode has to be the
contended stage, per-request load has to be heterogeneous enough that an equal count of
requests is not an equal load, and placement has to be committed in bulk (bursts rather
than a steady stream). Where prefill is the bottleneck instead, the same benchmark
measures the Coordinator's extra per-phase hop costing 4-8% of TTFT with nothing to show
for it. Worth stressing what the win is *not*: the sidecar's early pick is not working
from stale data, it is an exact in-process reservation made before prefill starts, so
the later pick adds information only when the pool's state actually changes in between.

### What the numbers don't cover

Two honest gaps remain. The first is that every run above, cost and win alike,
deliberately disables `conditional-decode` so that both arms prefill on every request.
That measures the Coordinator's worst case rather than its configured one; the fast path
can only improve on these numbers. The second is the
tokens-in path: the *size* half of that tradeoff, request bodies growing from kilobytes
to tens of megabytes as image resolution rises, is already measured in
`docs/coordinator_architecture.md`'s "Format tradeoff" section, but the wall-clock value
of tokenizing once is not yet quantified anywhere.

One secondary finding is worth reporting rather than burying: ITL spread is wider with
the Coordinator (p90-p10 of roughly 2.4-3.2ms vs. the sidecar's 0.8-1.5ms on the
input-length sweep, plus a heavier decode tail on the output-length sweep). It doesn't
move the median, and the linked analysis doesn't root-cause it.

## 6. What this unlocks beyond disaggregation

The modularity of sections 2 and 3 is the headline win available today. Everything
below is what the same pipeline model opens up next, each grounded in an existing design
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

## 7. Closing

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
- [ ] Measure the wall-clock value of tokenizing once (tokens-in vs. OpenAI-format);
      section 5 flags it as the one benchmark gap left.
- [ ] Section 5's deferred-decoding results come from llm-d#2658, still open. Either
      wait for the merge or label the numbers as pending review in the published text.
      Its three charts are pinned to that PR's head commit (`2b68d2f`); repoint them
      at `llm-d/llm-d/main` once it merges.
- [ ] Verify with EPP maintainers that per-frame `ext_proc` decisions genuinely don't
      exist, before the WebSocket point in section 6 goes out unchanged.
- [ ] Decide target venue/audience (llm-d blog vs. Anthropic-adjacent vs. CNCF); affects
      how much EPP/`ext_proc` background needs spelling out for readers unfamiliar with
      the project.
