import test from 'node:test';
import assert from 'node:assert/strict';
import { splitTrailers, TrailerParseError } from '../src/index.js';

/**
 * Build a ReadableStream<Uint8Array> from a list of Uint8Array chunks.
 * @param {Uint8Array[]} chunks
 * @returns {ReadableStream<Uint8Array>}
 */
function streamOf(chunks) {
  let i = 0;
  return new ReadableStream({
    pull(controller) {
      if (i < chunks.length) {
        controller.enqueue(chunks[i++]);
      } else {
        controller.close();
      }
    },
  });
}

function bytes(s) {
  return new TextEncoder().encode(s);
}

function concat(...parts) {
  let total = 0;
  for (const p of parts) total += p.length;
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.length;
  }
  return out;
}

test('splits a simple chunked body with trailers', async () => {
  const raw = concat(
    bytes('4\r\nWiki\r\n'),
    bytes('5\r\npedia\r\n'),
    bytes('0\r\n'),
    bytes('Expires: Wed, 21 Oct 2026 07:28:00 GMT\r\n'),
    bytes('X-Checksum: deadbeef\r\n'),
    bytes('\r\n')
  );
  const { body, trailers } = await splitTrailers(streamOf([raw]));
  assert.equal(new TextDecoder().decode(body), 'Wikipedia');
  assert.equal(trailers.size, 2);
  assert.equal(trailers.get('Expires'), 'Wed, 21 Oct 2026 07:28:00 GMT');
  assert.equal(trailers.get('X-Checksum'), 'deadbeef');
});

test('accepts a stream with no trailer section', async () => {
  const raw = concat(bytes('3\r\nabc\r\n'), bytes('0\r\n'));
  const { body, trailers } = await splitTrailers(streamOf([raw]));
  assert.equal(new TextDecoder().decode(body), 'abc');
  assert.equal(trailers.size, 0);
});

test('handles chunk boundaries splitting the trailer terminator', async () => {
  const raw = concat(
    bytes('2\r\nhi\r\n'),
    bytes('0\r\nX-A: 1\r'),
    bytes('\n\r\n')
  );
  const { body, trailers } = await splitTrailers(streamOf([raw]));
  assert.equal(new TextDecoder().decode(body), 'hi');
  assert.equal(trailers.get('X-A'), '1');
});

test('trims optional whitespace around trailer values', async () => {
  const raw = concat(
    bytes('1\r\nz\r\n'),
    bytes('0\r\n'),
    bytes('X-Padded:   spaced value  \r\n'),
    bytes('\r\n')
  );
  const { trailers } = await splitTrailers(streamOf([raw]));
  assert.equal(trailers.get('X-Padded'), 'spaced value');
});

test('ignores chunk extensions', async () => {
  const raw = concat(
    bytes('4;ext=1\r\nWiki\r\n'),
    bytes('0\r\n'),
    bytes('X-T: yes\r\n'),
    bytes('\r\n')
  );
  const { body, trailers } = await splitTrailers(streamOf([raw]));
  assert.equal(new TextDecoder().decode(body), 'Wiki');
  assert.equal(trailers.get('X-T'), 'yes');
});

test('rejects an invalid chunk size', async () => {
  const raw = bytes('zz\r\nhi\r\n0\r\n\r\n');
  await assert.rejects(() => splitTrailers(streamOf([raw])), TrailerParseError);
});

test('rejects a trailer line without a colon', async () => {
  const raw = concat(
    bytes('1\r\nx\r\n'),
    bytes('0\r\n'),
    bytes('NotAHeader\r\n'),
    bytes('\r\n')
  );
  await assert.rejects(() => splitTrailers(streamOf([raw])), TrailerParseError);
});

test('rejects a trailer name containing whitespace', async () => {
  const raw = concat(
    bytes('1\r\nx\r\n'),
    bytes('0\r\n'),
    bytes('Bad Name: value\r\n'),
    bytes('\r\n')
  );
  await assert.rejects(() => splitTrailers(streamOf([raw])), TrailerParseError);
});

test('rejects trailing bytes after the trailer section', async () => {
  const raw = concat(
    bytes('1\r\nx\r\n'),
    bytes('0\r\n'),
    bytes('A: b\r\n'),
    bytes('\r\n'),
    bytes('extra')
  );
  await assert.rejects(() => splitTrailers(streamOf([raw])), TrailerParseError);
});

test('rejects a chunk whose data is not followed by CRLF', async () => {
  const raw = bytes('2\r\nhiXX0\r\n\r\n');
  await assert.rejects(() => splitTrailers(streamOf([raw])), TrailerParseError);
});

test('last trailer wins when a name is repeated', async () => {
  const raw = concat(
    bytes('1\r\nx\r\n'),
    bytes('0\r\n'),
    bytes('X-Dup: one\r\n'),
    bytes('X-Dup: two\r\n'),
    bytes('\r\n')
  );
  const { trailers } = await splitTrailers(streamOf([raw]));
  assert.equal(trailers.get('X-Dup'), 'two');
});

test('rejects a non-stream argument', async () => {
  await assert.rejects(() => splitTrailers(null), TrailerParseError);
});
