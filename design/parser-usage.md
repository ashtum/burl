# Using the burl parser

The parser is split in two. `parser` performs no I/O: you hand it octets
and call step functions. `message_reader` binds a parser to a stream and
runs that loop for you.

Everything below compiles against `<boost/burl/parser.hpp>` and
`<boost/burl/message_reader.hpp>`.

## Layer 1 — `parser`

Octets go in through two calls, and come out through three, depending on
where you want them to land.

| Input | |
| --- | --- |
| `prepare()` | `array<mutable_buffer, 2>` to write into |
| `commit(n)` | report octets written |
| `commit_eof()` | the stream closed |
| `direct_capacity()` | octets that may bypass the buffer, or 0 |
| `commit_direct(n)` | report octets written straight to your memory |

| Output | |
| --- | --- |
| `parse_header(ec)` | |
| `body(ec)` | the whole body in place, no copy |
| `read_some(bufs, ec)` | copy into your memory |
| `pull(dest, ec)` / `consume(n)` | borrow the parser's buffers |

Every step function reports `http::error::need_data` when it wants more
input. That is the entire protocol:

```cpp
response_parser pr( response_parser::config{} );
pr.start();

std::string_view src =
    "HTTP/1.1 200 OK\r\n"
    "Content-Length: 5\r\n"
    "\r\n"
    "hello";

// Copy from src into whatever room the parser currently has.
auto feed = [&]
{
    std::size_t n = 0;
    for(auto b : pr.prepare())
    {
        auto const k = std::min( b.size(), src.size() );
        std::memcpy( b.data(), src.data(), k );
        src.remove_prefix( k );
        n += k;
    }
    if(n == 0)
        pr.commit_eof();
    else
        pr.commit( n );
};

for(;;)
{
    system::error_code ec;
    pr.parse_header( ec );
    if(!ec)
        break;
    if(ec != http::error::need_data)
        throw std::system_error( ec );
    feed();
}

std::cout << pr.get().status_int() << " " << pr.get().reason() << "\n";

for(;;)
{
    system::error_code ec;
    auto const sv = pr.body( ec );
    if(!ec)
    {
        std::cout << sv;            // "hello"
        break;
    }
    if(ec != http::error::need_data)
        throw std::system_error( ec );
    feed();
}
```

No socket, no coroutine, no allocation on the loop. The same code drives
a file, a test vector, or a TLS record layer.

Note the header loop is not optional at this layer: `body`, `read_some`,
and `pull` all assert `got_header()`. Sequencing is the caller's job here
— `message_reader` is where it becomes automatic.

The other errors say why more input will not help:
`in_place_overflow` (no writable space left), `incomplete` (`commit_eof`
already called), `end_of_stream` (closed cleanly before the message
began).

## Layer 2 — `message_reader`

Binds a stream to a parser, by pointer, and holds nothing else:

```cpp
response_parser pr( response_parser::config{} );
message_reader reader( &sock, &pr );

pr.start();

if(auto [ec] = co_await reader.read_header(); ec)
    co_return { ec };
```

Pick the body operation by where the octets should end up.

**The whole body, no copy** — bounded by `in_buffer`, fails with
`in_place_overflow` if it does not fit:

```cpp
auto [ec, body] = co_await reader.read_body();
```

**Borrow the parser's buffers** — zero copy, for a body of any size:

```cpp
capy::const_buffer bufs[ 8 ];
for(;;)
{
    auto [ec, data] = co_await reader.pull( bufs );
    if(ec == capy::cond::eof)
        break;
    if(ec)
        co_return { ec };
    write_to_file( data );
    reader.consume( capy::buffer_size( data ));
}
```

**Into your own memory** — `read_some` for one chunk, `read` to fill the
sequence:

```cpp
char buf[ 16 * 1024 ];
for(;;)
{
    auto [ec, n] = co_await reader.read_some(
        capy::mutable_buffer( buf, sizeof(buf) ));
    if(ec == capy::cond::eof)
        break;
    if(ec)
        co_return { ec };
    process( buf, n );
}
```

`read_some` is not `pull` plus a copy. When the parser's buffer is empty
and the framing allows it, it reads from the stream *directly into your
buffer* and the parser never touches the octets — see `direct_capacity`.
A 4 MB body can move through a 64 KB parser in one read.

Every body operation drives `read_header` first, so the reader above
works without the explicit call. That serves the caller with no interest
in the header; anything decided *from* the header needs the explicit
call, because `set_decoder` requires a parsed header and an untouched
body.

## Decoders

A decoder transforms payload octets as they arrive. It performs no I/O
and does not allocate on the parser's behalf:

```cpp
struct parser::decoder
{
    virtual result process(
        capy::mutable_buffer out,
        capy::const_buffer in,
        bool eof) = 0;    // -> { consumed, produced, ec }
};
```

Install it in the window between the header and the first body octet:

```cpp
co_await reader.read_header();

gzip_decoder dec;
if(pr.get().value_or( http::field::content_encoding, "" ) == "gzip")
    pr.set_decoder( &dec );

auto [ec, body] = co_await reader.read_body();   // decoded
```

`read_some` then lets the decoder write into the caller's buffer
directly, so decoding costs no extra copy.

## Message boundaries

`start()` prepares for the next message and keeps octets already received
that belong to it, which is what makes a pipelined connection work. It
does **not** drain: reaching `got_body()` is the caller's job.

```cpp
while(reuse)
{
    pr.start();
    co_await reader.read_header();
    ...
}
```

`buffered_data()` returns the raw buffer contents as
`array<const_buffer, 2>`. Use it to recover octets the parser is holding
that are not body — a tunnel's first bytes after a `CONNECT`, where the
framing verdict does not apply.

## Conformance

`message_reader` satisfies `capy::ReadStream`, `http::ReadSource`, and
`http::BufferSource`, all over the body octets, so it drops into anything
written against those — including the type-erased
`http::any_buffer_source`.
