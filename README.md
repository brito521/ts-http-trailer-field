# ts-http-trailer-field

Splits a chunked transfer response body from its trailing headers when using fetch streams.

```js
import { splitTrailers } from 'ts-http-trailer-field';

// A chunked body with a trailer section, as bytes.
const raw = new TextEncoder().encode(
  '4\r\nWiki\r\n5\r\npedia\r\n0\r\nX-Checksum: deadbeef\r\n\r\n'
);
const stream = new ReadableStream({
  start(controller) {
    controller.enqueue(raw);
    controller.close();
  },
});

const { body, trailers } = await splitTrailers(stream);

console.log(new TextDecoder().decode(body)); // "Wikipedia"
console.log(trailers.get('X-Checksum')); // "deadbeef"
```

## Why this exists

When a server uses chunked transfer coding, it can append a trailer section after the body. `fetch` hands you a single `ReadableStream<Uint8Array>` that contains both, and there is no standard method that separates them. This library consumes that stream and returns the decoded body bytes plus the trailer fields as a `Map`.

The trade-off is that the whole stream is buffered before parsing. A true streaming parser would keep a small state machine and emit body chunks as they arrive, but the trailer terminator can straddle a chunk boundary, and the bookkeeping for that is easy to get subtly wrong. Buffering keeps the parser honest and the tests deterministic; it is the right choice for responses where trailers matter (checksums, signatures) rather than for multi-gigabyte downloads.

## Edge case you will hit

A stream that ends immediately after the final chunk's CRLF, with no trailer section at all, is accepted and yields an empty `Map`. That is the common case when a server sends chunked encoding without trailers, and rejecting it would make the library useless for most responses. A stream with any bytes after the terminating blank line raises `TrailerParseError`.

## API

`splitTrailers(stream)` returns a `Promise<{ body: Uint8Array, trailers: Map<string, string> }>`.

The parser requires CRLF line endings. A chunk-size line may carry a chunk extension after a semicolon; the extension is ignored. Trailer values are trimmed of surrounding spaces and tabs. If a trailer name appears more than once, the last occurrence wins.

`TrailerParseError` is exported for callers that want to catch parse failures specifically.
