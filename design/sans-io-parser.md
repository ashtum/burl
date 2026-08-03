# Sans-I/O parser design

Status: implemented.
Scope: replace `burl::detail::parser`'s coroutine-based I/O with a sans-I/O state
machine plus free-function drivers, and promote the result to public API.

## Motivation

`detail::parser` currently owns a `capy::any_read_stream` and performs I/O inside
every operation (`read_header`, `read_body`, `read_some`, `read`, `pull`). That
couples the framing state machine to capy, prevents use from any other I/O model,
and keeps the parser out of the public API.

The decoder interface, the buffer strategy, and the three-way body delivery model
are the parts of the current design worth preserving verbatim. Only the I/O half
moves out.

## Comparison with `boost::http::parser`

|                     | `boost::http::parser`                                          | `burl::detail::parser`                                                            |
| ------------------- | -------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| I/O coupling        | sans-I/O core; I/O as member templates on `Stream`             | owns `capy::any_read_stream`; every operation is a coroutine                       |
| Drive surface       | `prepare` / `commit` / `commit_eof` / `parse(ec)`              | none — driven implicitly by the `read_*` coroutines                                |
| Header              | folded into `parse()`                                          | separate sans-I/O `burl::head_parser` (`prepare` / `commit` / `parse`)             |
| Body delivery       | one path: everything lands in the parser's buffer              | three paths: in place, into caller buffers, borrowed                               |
| Zero copy           | no — `read(stream, buffers)` copies out of the internal buffer | yes — reads straight into caller buffers for `payload::size` / `to_eof`            |
| Decoders            | internal, built into `impl`                                    | public sans-I/O extension point (`decoder::process`), injected via `set_decoder`   |
| Chunked de-framing  | inside `parse()`                                               | `walk_chunks` (streaming) + `flatten_chunks` (in-place coalescing for whole-body)  |
| Descriptor storage  | `mbp_` / `cbp_` arrays held in `impl`                          | none — buffers returned by value or filled into caller-provided spans              |
| Need-more signal    | `condition::need_more_input` from `parse`                      | `http::error::need_data`, absorbed internally by `refill()`                        |

The structural point: boost.http's single `parse()` **forces** the "all body
through my buffer" model, which is why its `read(stream, buffers)` cannot avoid a
copy. burl's per-operation dispatch is what buys the zero-copy path, so the
sans-I/O split keeps **one step function per operation** rather than collapsing
to a single `parse()`.

## Architecture

Two layers.

**Layer 1** — `burl::parser`, compiled, sans-I/O, holds no buffer-descriptor
state. Owns all framing, limit, and error policy. Never suspends, never touches a
stream.

**Layer 2** — `message_reader<S>`, header-only, a two-pointer binding of a stream
to a parser. Owns nothing but the read loop. All five loops share one `refill_impl`
helper.

Every site that touches `stream_` today is confined to four places in
`src/detail/parser.cpp`: the read in `read_header`, `refill()`, and the two
direct reads in `do_read_some`. Those become layer 2; everything else stays put.

## Layer 1: `burl::parser`

```cpp
class parser
{
public:
    struct decoder                                  // unchanged
    {
        struct result
        {
            std::size_t consumed;
            std::size_t produced;
            std::error_code ec;
        };

        virtual ~decoder() = default;

        virtual result
        process(
            capy::mutable_buffer out,
            capy::const_buffer in,
            bool eof) = 0;
    };

    struct config                                   // unchanged
    {
        header_limits hdr_limits;

        std::size_t in_buffer    = 64 * 1024;
        std::size_t dec_buffer   =  8 * 1024;
        std::uint64_t body_limit = std::uint64_t(-1);
    };

    // ---- observers ----

    bool got_header() const noexcept;
    bool got_body() const noexcept;
    bool has_buffered_data() const noexcept;

    // Raw unconsumed bytes in the input buffer. Valid after got_header().
    std::array<capy::const_buffer, 2> buffered_data() const noexcept;

    // ---- lifecycle ----

    void reset() noexcept;                          // no stream parameter
    void set_decoder(decoder* dec) noexcept;
    void set_body_limit(std::uint64_t n) noexcept;

    // ---- input side ----

    std::array<capy::mutable_buffer, 2> prepare() noexcept;
    void commit(std::size_t n) noexcept;
    void commit_eof() noexcept;

    // ---- zero-copy pass-through (layer 2 only) ----

    std::size_t direct_capacity() const noexcept;
    void commit_direct(std::size_t n) noexcept;

    // ---- steps (one per former coroutine) ----

    void parse_header(system::error_code& ec);
    std::string_view body(system::error_code& ec);
    std::size_t read_some(
        std::span<capy::mutable_buffer const> buffers,
        system::error_code& ec);
    std::span<capy::const_buffer> pull(
        std::span<capy::const_buffer> dest,
        system::error_code& ec);
    void consume(std::size_t n) noexcept;

protected:
    parser() = default;
    parser(config const& cfg, bool is_request);
    parser(parser&& other) noexcept = default;
    parser& operator=(parser&& other) noexcept = default;

    void start(bool head);                          // unchanged

    burl::request_head_base const& get_request() const;
    burl::response_head_base const& get_response() const;
};
```

`request_parser` / `response_parser` keep their current shape (public derived
classes over a protected base) and move to public headers alongside it.

### Input side

`prepare()` dispatches on `got_header_`: before the header it returns
`hp_.prepare()` with an empty second region; after, `in_.prepare()`. Returning by
value rather than a span is deliberate — the two-region count is structural to a
circular buffer, and it keeps the parser free of the descriptor arrays boost.http
has to carry (`mbp_`, `cbp_`).

`commit(n)` absorbs `refill()`'s bookkeeping: sets `got_body_` when
`payload_sized() && payload_rem() <= in_.size()`.

`commit_eof()` sets `eof_`, and `got_body_` when `payload_ == payload::to_eof`.

`refill()`'s error returns (`in_place_overflow` when the buffer is full,
`incomplete` when already at eof) become results of the step functions instead.

### Zero-copy pass-through

`direct_capacity()` returns a byte budget for reading straight from the stream
into caller memory, or `0` when that is not permitted. It is `0` unless **all**
of:

- `got_header()`
- no decoder installed
- `payload_` is `size` or `to_eof`
- `in_` is empty
- `!eof_`
- `raw_limit_rem() != 0`

The budget is `clamp(payload_rem(), raw_limit_rem())` for `payload::size` and
`raw_limit_rem()` for `to_eof`. Because it is clamped, the body limit can never
be exceeded by a direct read; `body_too_large` still surfaces on the following
call, when `raw_limit_rem()` has reached zero.

`commit_direct(n)` does `transferred_ += n` and sets `got_body_` when a sized
payload is complete.

The `in_.empty()` requirement is load-bearing: `has_buffered_data()` for
`payload::size` is `in_.size() > payload_rem()`, which stays correct only because
directly-read bytes never enter `in_`.

### Message boundaries

`start(bool head)` is where preparation for the next message lives, and it is
untouched by this design: it performs no I/O today. It stays protected, exposed
publicly by the derived classes as `request_parser::start()` and
`response_parser::start(bool head = false)`.

What it does, unchanged: consume the unread remainder of a sized payload,
`move_leftovers` the buffered tail to the front of the buffer, hand that tail to
`hp_.reset(in_.size())` so the head parser sees it as already committed, and clear
the per-message state. `body_limit_` is deliberately not cleared — see the sticky
body limit decision below.

Its precondition is `!started_ || got_body_`, and it is worth stating explicitly
in the public docs that **`start()` does not drain**: `in_.consume(payload_rem())`
clamps to what is actually buffered, so an unread body still on the wire is not
skipped. Reaching `got_body()` is the caller's job — which is what `drain_body`
does and what `can_reuse_conn` checks before returning a connection to the pool.

### `buffered_data()` and tunnels

`buffered_data()` is deliberately raw — `in_.data()`, with no framing-aware
skipping — because for the case that needs it the parser's framing verdict is
itself wrong.

`message_head_base::payload()` has no knowledge of the request method, so a
`200 Connection Established` response to CONNECT, carrying neither Content-Length
nor Transfer-Encoding, is classified `payload::to_eof` and not `payload::none`.
Two consequences:

- `has_buffered_data()` returns `false` unconditionally for `to_eof`, so it will
  actively hide any bytes the proxy sent after the response.
- The caller, which knows it issued a CONNECT, knows there is no body regardless.

So raw `buffered_data()` is the only accessor that can hand back a tunnel's
opening bytes. The cost of being raw is that for a partially-read sized body it
also includes the unread body tail; that is documented rather than fixed.

Related latent hole, pre-existing and now fixable: `http_tunnel.cpp` never
inspects leftovers, so bytes buffered past the CONNECT response are dropped when
the parser is destroyed. Benign in practice because the client speaks first after
CONNECT (the TLS ClientHello), so a well-behaved proxy has sent nothing yet.

### Error convention

Every step function reports through a `system::error_code&` out parameter,
following `head_parser`'s existing contract:

- `http::error::need_data` — fill `prepare()`, `commit`, call again. Returned
  **only** when `prepare()` has room.
- `http::error::in_place_overflow` — more input needed but no writable space.
- `http::error::incomplete` — more input needed but `commit_eof` has been called.
- `http::error::end_of_stream` — clean eof before any header byte arrived.

The driver therefore never inspects buffer fullness or eof state; it branches on
`need_data` and returns everything else. All framing and limit policy stays in
the compiled layer.

**Bytes-or-error rule.** Layer 1 returns bytes or an error, never both. Layer 2
may return both (see `read_some` below). This is the difference between "retry is
safe" and "you must account for the bytes first", so it belongs in the docs.

### Why `parse_header` stays separate

The whole burl design depends on stopping after the header: `set_decoder` asserts
`transferred_ == 0`, and `client.cpp` inspects the status, harvests `set-cookie`,
decides redirects, and only then installs the decoder — all between the header
and the first body byte. Folding header parsing into the body steps leaves
nowhere to stand. (boost.http's `parse()` returns early after the header for the
same reason.)

Division of labour: layer-1 body steps assert `got_header()`; the layer-2 loops
`read_body_impl` / `read_some_impl` / `pull_impl` each drive `read_header_impl`
first, exactly as the current coroutines do. So `as_buffer_source()` still works
with no explicit header read, while `message_reader::read_header()` remains
available for callers that need to intervene.

## Layer 2: `message_reader`

A single public type: two pointers binding a stream to a parser, cheap enough to
construct on demand for one operation. The public surface is the instance members;
each is a one-line forward to a private static carrying the same name plus
`_impl`. There are no namespace-scope functions.

```cpp
template<capy::ReadStream S>
class message_reader
{
    S* s_;
    parser* p_;

    // the loops: coroutines, no `this`
    static capy::io_task<> refill_impl(S& s, parser& p);
    static capy::io_task<> read_header_impl(S& s, parser& p);
    static capy::io_task<std::string_view> read_body_impl(S& s, parser& p);
    static capy::io_task<std::span<capy::const_buffer>>
        pull_impl(S& s, parser& p, std::span<capy::const_buffer> dest);
    template<capy::MutableBufferSequence MB>
        static capy::io_task<std::size_t> read_some_impl(S& s, parser& p, MB b);
    template<capy::MutableBufferSequence MB>
        static capy::io_task<std::size_t> read_impl(S& s, parser& p, MB b);

public:
    message_reader(S& s, parser& p) noexcept;

    // never coroutines — see below
    capy::io_task<> read_header()               { return read_header_impl(*s_, *p_); }
    capy::io_task<std::string_view> read_body()  { return read_body_impl(*s_, *p_); }
    void consume(std::size_t n) noexcept         { p_->consume(n); }

    capy::io_task<std::span<capy::const_buffer>>
    pull(std::span<capy::const_buffer> dest)     { return pull_impl(*s_, *p_, dest); }

    template<capy::MutableBufferSequence MB>
    capy::io_task<std::size_t> read_some(MB buffers)
    {
        return read_some_impl(*s_, *p_, std::move(buffers));
    }

    template<capy::MutableBufferSequence MB>
    capy::io_task<std::size_t> read(MB buffers)
    {
        return read_impl(*s_, *p_, std::move(buffers));
    }
};
```

Distinct `_impl` names rather than static/non-static overloads of the same name:
the overload is legal when the parameter lists differ, but there is no reason to
lean on that, and it keeps a single public spelling per operation. `S` never needs
deducing, since nothing outside the class names a static.

The static bodies are the loops. `read_body_impl` is representative:

```cpp
template<capy::ReadStream S>
capy::io_task<std::string_view>
message_reader<S>::read_body_impl(S& stream, parser& pr)
{
    for(;;)
    {
        system::error_code ec;
        auto sv = pr.body(ec);
        if(ec != http::error::need_data)
            co_return { ec, sv };
        if(auto [rec] = co_await refill_impl(stream, pr); rec)
            co_return { rec, {} };
    }
}

template<capy::ReadStream S>
capy::io_task<>
message_reader<S>::refill_impl(S& stream, parser& pr)
{
    auto [ec, n] = co_await stream.read_some(pr.prepare());
    if(ec == capy::cond::eof)  pr.commit_eof();
    else if(ec)                co_return { ec };
    else                       pr.commit(n);
    co_return {};
}
```

### Why the statics exist

Not for the call syntax — for lifetime. Because the reader is cheap, callers will
construct it as a temporary, and a coroutine member captures `this`. The task then
outlives the object it points at as soon as it is stored rather than awaited in
place:

```cpp
auto t = message_reader{s, p}.read_body();   // temporary dies here
co_await t;                                  // ... `this` dangles
```

A static has no `this`; its frame holds `S&` and `parser&`, bound to objects that
outlive the task. Forwarding to it leaves nothing pointing at the temporary. Hence
the rule: **no public member of `message_reader` is a coroutine.**

This is what makes the existing `response::try_as_view` shape keep working with a
temporary reader, since the task it hands to `corosio::timeout` refers to the
long-lived `stream_` and `parser_` rather than to the reader:

```cpp
co_return co_await corosio::timeout(
    message_reader{ stream_, parser_ }.read_body(),
    *deadline_ - clock::now());
```

The same trap catches `read`, which cannot be the obvious one-liner. Today
`parser::read` is `capy::read(*this, ...)`, safe only because `*this` is the
long-lived parser. `capy::read` takes its stream **by reference** and holds that
reference in its frame — documented as load-bearing at `capy/read.hpp:61-62` — so
handing it a temporary reader's `*this` dangles.

Rather than give the reader a home in the static's frame just to satisfy
`capy::read`, `read_impl` runs the fill loop itself. That drops one coroutine frame
per `read` call and removes the dependency entirely:

```cpp
template<capy::ReadStream S>
template<capy::MutableBufferSequence MB>
capy::io_task<std::size_t>
message_reader<S>::read_impl(S& stream, parser& pr, MB buffers)
{
    auto const total_size = capy::buffer_size(buffers);
    capy::consuming_buffers dest(buffers);
    std::size_t total = 0;

    while(total < total_size)
    {
        auto [ec, n] = co_await read_some_impl(stream, pr, dest.data());
        dest.consume(n);
        total += n;
        // a contingency that still completed the transfer is a success
        if(ec && total < total_size)
            co_return { ec, total };
    }

    co_return { {}, total };
}
```

This reproduces `capy::read`'s semantics, including the empty-sequence case (no
I/O, not a contingency) and the rule that an error accompanying the read which
fills the sequence is not reported. It also preserves what callers already depend
on: a body shorter than the destination yields `{capy::error::eof, total}`, which
is what `string.cpp` clears when converting a body to `std::string`.

### `read_some_impl` — the only non-trivial loop

It is the sole consumer of `direct_capacity` / `commit_direct`, and the commit
ordering matters:

```cpp
template<capy::ReadStream S>
template<capy::MutableBufferSequence MB>
capy::io_task<std::size_t>
message_reader<S>::read_some_impl(S& stream, parser& pr, MB buffers)
{
    if(auto [ec] = co_await read_header_impl(stream, pr); ec)
        co_return { ec, 0 };

    capy::buffer_param bp(buffers);

    for(;;)
    {
        system::error_code ec;
        auto const n = pr.read_some(bp.data(), ec);
        if(ec != http::error::need_data)
            co_return { ec, n };

        // nothing buffered; may we read into the caller's memory directly?
        if(auto const lim = pr.direct_capacity(); lim != 0)
        {
            auto [rec, rn] = co_await stream.read_some(
                capy::buffer_slice(bp.data(), 0, lim));

            pr.commit_direct(rn);           // ALWAYS, before inspecting rec
            if(rec == capy::cond::eof)
                pr.commit_eof();
            else if(rec)
                co_return { rec, rn };      // hand the bytes back with the error
            if(rn != 0)
                co_return { {}, rn };
            continue;                       // let layer 1 name the error
        }

        if(auto [rec] = co_await refill_impl(stream, pr); rec)
            co_return { rec, 0 };
    }
}
```

`commit_direct` must precede the `rec` check. A cancelled or failed
`stream.read_some` can complete with `(ec, n > 0)` — `corosio::timeout` firing
mid-read makes this reachable — and those bytes are already in the caller's
buffer. Skipping the commit desyncs `transferred_`, after which the connection
looks reusable when it is not. This mirrors the current code, which does
`transferred_ += n` before testing `ec` and returns `{incomplete, n}`.

Behaviour change worth noting: a short read at eof now surfaces the bytes first
and `incomplete` on the *next* call, where today both come back together.

`refill_impl` needs the same discipline, and for the same reason — a read which
delivers the last octets together with eof must commit them before acting on the
contingency:

```cpp
auto [ec, n] = co_await stream.read_some(pr.prepare());
pr.commit(n);                           // always, before inspecting ec
if(ec == capy::cond::eof)
{
    pr.commit_eof();
    co_return {};
}
if(ec)
    co_return { ec };
co_return {};
```

This is what makes the `payload::to_eof` reordering in `parser::read_some`
necessary rather than merely tidier: once refill can leave `in_` non-empty *and*
`eof_` set, the old order (`if(eof_) return eof` before looking at `in_`) drops
those octets. The two changes only work together.

Neither `capy::test::read_stream` nor `capy::test::stream` ever reports eof
alongside data — both drain first and report eof on a following read — so this
case needs its own test double. `test/unit/parser.cpp` has `eager_eof_stream`
for it; without it the suite passes with the octets silently dropped.

### Why `read_some` is not `pull` plus a copy

Three of its four paths copy less than a `pull`-based shim would:

- **Sized / to-eof, no decoder** — reads straight into the caller's buffers, zero
  copies, and is not capped by `in_buffer` per round trip. A 10 MB destination is
  one syscall; pull-and-copy is 64 KB at a time plus a memcpy each lap.
- **With a decoder** — the decoder writes its output directly into the caller's
  buffers, bypassing `out_`. Over `pull` it must land in `out_` first and be
  copied out: two copies instead of one, each lap capped at `dec_buffer`. This is
  the default path for `co_await r.as<std::string>()`, since the client installs
  a decoder whenever the user did not set `accept-encoding`.
- **Scatter-gather** — fills a fragmented `MutableBufferSequence` in one call.

Only the chunked, non-decoded path really is walk-chunks plus `buffer_copy`.

(The name `read_some` is kept for that reason: `copy_body` would be wrong for the
decoded case, where nothing is copied.)

### Concept conformance

One type satisfies all three concepts at once, because their required members do
not overlap:

| Concept | Requires |
| --- | --- |
| `capy::ReadStream` | `read_some(MB)` |
| `http::ReadSource` | `capy::ReadStream` + `read(MB)` |
| `http::BufferSource` | `pull(span<const_buffer>)` + `consume(n)` |

So `response::as_buffer_source()` and `as_read_source()` wrap the same type, each
type-erasing it separately, and no per-concept adapters are needed.

Observers and per-message setup stay on the parser: a caller holding a reader
necessarily holds the parser it was built from, so there is nothing to forward.

`pull` keeps its span form rather than returning a single buffer. A single-buffer
step would force `pull` to return one-element spans, since two consecutive `pull`s
without an intervening `consume` return the same run — that would cost vectored
relay (two `write_some` calls across a circular-buffer wrap instead of one). The
span form also lets `collect()` stay as is.

## Migration

| Site | Change |
| --- | --- |
| `include/boost/burl/detail/{parser,request_parser,response_parser}.hpp` | move to public `include/boost/burl/`; drop `stream_`, add the input side and steps |
| `src/detail/parser.cpp` | delete the four stream touch points; coroutines become step functions |
| `src/client.cpp` | `parser.reset(std::move(stream))` → `parser.reset()`; `parser.read_header()` → `message_reader{stream, parser}.read_header()` |
| `src/detail/http_tunnel.cpp` | `response_parser parser({}, &stream)` → `parser({})`; `message_reader{stream, parser}.read_header()` |
| `src/detail/drain_body.cpp` | `drain_body(parser, attempts)` gains a stream parameter |
| `include/boost/burl/response.hpp` | add `capy::any_stream stream_` after `conn_`; move it in the move ops |
| `src/response.cpp` | `try_as_view` → `message_reader{stream_, parser_}.read_body()`; `as_buffer_source` / `as_read_source` → wrap `message_reader` |
| `include/boost/burl/test/response_factory.hpp` | `response_parser parser({}, conn.stream())` → `parser({})`; drive the header read with a local stream |
| `src/detail/can_reuse_conn.cpp` | unchanged |

`capy::any_read_stream` disappears from burl entirely. `capy::any_stream` derives
from `any_read_stream`, so it already satisfies `capy::ReadStream` and drops
straight into the layer-2 templates; `response` stores the same type the pool
hands out. That also removes a latent hazard: today `parser.reset(std::move(stream))`
passes an `any_stream` to an `any_read_stream` parameter — a slicing move that
leaves `any_stream::storage_` / `destroy_` in the local, which then destructs.
It is benign only because `pooled_connection::stream()` returns the non-owning
pointer form, and would break the day the pool hands out an owning stream.

## Implementation notes

Two things surfaced while building this which are not visible from the design
alone.

**`response_parser parser({})` became ambiguous.** With the stream argument gone,
a braced empty argument matches both the default constructor and the `config`
one. Call sites now name the type: `response_parser parser(response_parser::config{})`.

**`capy::buffer_slice` rejects rvalue sequences** (`BufferSequence const&&` is
deleted, to stop callers slicing a temporary). The direct-read path has to name
the window before slicing it:

```cpp
auto const mbs = bp.data();
auto [rec, rn] = co_await stream.read_some(capy::buffer_slice(mbs, 0, lim));
```

**A latent bug fixed in passing.** `payload::to_eof` in the old `do_read_some`
tested `eof_` before looking at `in_`, so a read which found the stream ended
would report eof and abandon whatever was still buffered. It was unreachable
through `do_read_some` alone, because that path only buffered header leftovers,
but reachable by mixing `pull` with `read_some`.

## Decisions

| Question | Decision |
| --- | --- |
| Keep the direct-read fast path? | Yes, via `direct_capacity()` / `commit_direct()` |
| Stream type in the drivers | Template on `capy::ReadStream` |
| Internal refactor or public API? | Public `burl::` API |
| Error convention | `system::error_code&` out parameter, `need_data` (head_parser's contract) |
| `prepare()` return | `std::array<capy::mutable_buffer, 2>` by value — no internal descriptors |
| `pull()` shape | Keep the caller-provided span; a single-buffer step would force `message_reader::pull` to return one-element spans |
| Keep `read_some`? | Yes — it is not pull-plus-copy on three of four paths |
| Keep `parse_header` separate? | Yes — `set_decoder` and header inspection need the stop |
| Adapters or free functions? | Neither: one `message_reader<S>` satisfying all three concepts, public instance members forwarding to private `_impl` statics — the statics are what make it safe as a temporary |
| `start()` | Unchanged and protected, exposed by the derived classes; does not drain |
| `buffered_data()` | `return in_.data();` — raw, valid after `got_header()` |
| Sticky `body_limit` | Intentional: neither `start()` nor `reset()` clears it, unlike boost.http |

## Deferred

- **Chunked trailers.** `skip_trailer` discards them; no storage, no accessor.
- **`buffered_data()` semantics for a partially-read sized body.** It is raw, so
  it includes the unread body tail. Documented rather than fixed; see the tunnel
  discussion above for why raw is the right default.
- **`release_buffered_data()`.** boost.http declares one and left it a stub;
  `buffered_data()` covers the read side for now.
- **`http_tunnel.cpp` dropping bytes buffered past the CONNECT response.** Now
  detectable via `buffered_data()`; not addressed here.
