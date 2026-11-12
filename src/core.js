/**
 * Splits a chunked transfer response body from its trailing headers when using
 * fetch streams.
 *
 * The HTTP/1.1 chunked transfer coding allows a message body to be followed by
 * a trailer section: zero or more header fields that are sent after the body.
 * `fetch` gives us a `ReadableStream<Uint8Array>` for the whole response, with
 * no built-in way to tell where the body ends and the trailers begin. This
 * module consumes the stream and returns the body bytes together with the
 * parsed trailer fields.
 *
 * The parser is deliberately strict: it requires the final chunk to be
 * followed by a CRLF, then the trailer lines, then a terminating CRLF. A
 * response that ends immediately after the final chunk (no trailer section at
 * all) is accepted and yields an empty trailer map. Anything else raises
 * `TrailerParseError`.
 */

/**
 * Error thrown when the byte stream is not a valid chunked body with trailers.
 */
export class TrailerParseError extends Error {
  /**
   * @param {string} message
   */
  constructor(message) {
    super(message);
    this.name = 'TrailerParseError';
  }
}

const CR = 0x0d;
const LF = 0x0a;
const SP = 0x20;
const HTAB = 0x09;
const COLON = 0x3a;

/**
 * Concatenate the chunks of a ReadableStream<Uint8Array> into a single
 * Uint8Array. We do this because the trailer terminator can straddle a chunk
 * boundary, and a streaming parser would need to keep a small state machine
 * for that. For the response sizes this library targets (headers and trailers,
 * not multi-gigabyte bodies) buffering is simpler and easier to reason about.
 *
 * @param {ReadableStream<Uint8Array>} stream
 * @returns {Promise<Uint8Array>}
 */
async function readAll(stream) {
  if (!stream || typeof stream.getReader !== 'function') {
    throw new TrailerParseError('expected a ReadableStream<Uint8Array>');
  }
  const reader = stream.getReader();
  /** @type {Uint8Array[]} */
  const parts = [];
  let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!(value instanceof Uint8Array)) {
        throw new TrailerParseError('stream yielded a non-Uint8Array chunk');
      }
      parts.push(value);
      total += value.length;
    }
  } finally {
    reader.releaseLock();
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}

/**
 * Parse a single trailer header line. Returns null for an empty line, which
 * signals the end of the trailer section.
 *
 * @param {Uint8Array} bytes
 * @param {number} start
 * @param {number} end
 * @returns {{ name: string, value: string } | null}
 */
function parseTrailerLine(bytes, start, end) {
  if (start === end) return null;

  let colon = -1;
  for (let i = start; i < end; i++) {
    if (bytes[i] === COLON) {
      colon = i;
      break;
    }
  }
  if (colon === -1) {
    throw new TrailerParseError('trailer line is missing a colon');
  }

  // Field names must not contain whitespace. We reject leading/trailing space
  // because a lenient parser here would silently accept malformed trailers and
  // make the failure show up much later in the caller's code.
  const nameBytes = bytes.subarray(start, colon);
  for (let i = 0; i < nameBytes.length; i++) {
    const b = nameBytes[i];
    if (b === SP || b === HTAB) {
      throw new TrailerParseError('trailer name contains whitespace');
    }
  }
  if (nameBytes.length === 0) {
    throw new TrailerParseError('trailer line has an empty name');
  }

  // Trim optional whitespace (SP / HTAB) around the field value.
  let vStart = colon + 1;
  while (vStart < end && (bytes[vStart] === SP || bytes[vStart] === HTAB)) {
    vStart++;
  }
  let vEnd = end;
  while (vEnd > vStart && (bytes[vEnd - 1] === SP || bytes[vEnd - 1] === HTAB)) {
    vEnd--;
  }

  const decoder = new TextDecoder('utf-8', { fatal: false });
  const name = decoder.decode(nameBytes);
  const value = decoder.decode(bytes.subarray(vStart, vEnd));
  return { name, value };
}

/**
 * Split a chunked transfer response body from its trailing headers.
 *
 * The stream must contain a complete HTTP/1.1 chunked body. The final chunk is
 * followed by a CRLF, then zero or more trailer lines, then a terminating CRLF.
 * A stream that ends right after the final chunk (no trailer section) is also
 * accepted.
 *
 * @param {ReadableStream<Uint8Array>} stream
 * @returns {Promise<{ body: Uint8Array, trailers: Map<string, string> }>}
 */
export async function splitTrailers(stream) {
  const bytes = await readAll(stream);
  const len = bytes.length;
  let pos = 0;

  /** @type {Uint8Array[]} */
  const bodyParts = [];
  let bodyLength = 0;

  for (;;) {
    // Read the chunk-size line up to CRLF.
    let lineEnd = -1;
    for (let i = pos; i + 1 < len; i++) {
      if (bytes[i] === CR && bytes[i + 1] === LF) {
        lineEnd = i;
        break;
      }
    }
    if (lineEnd === -1) {
      throw new TrailerParseError('unterminated chunk-size line');
    }

    const sizeLine = bytes.subarray(pos, lineEnd);
    pos = lineEnd + 2;

    // A chunk-size line may carry a chunk extension after a semicolon. We
    // ignore the extension but still require the size itself to be hex.
    let sizeEnd = sizeLine.length;
    for (let i = 0; i < sizeLine.length; i++) {
      if (sizeLine[i] === 0x3b) {
        sizeEnd = i;
        break;
      }
    }
    const sizeText = new TextDecoder('ascii', { fatal: false }).decode(
      sizeLine.subarray(0, sizeEnd)
    ).trim();
    if (sizeText.length === 0) {
      throw new TrailerParseError('empty chunk-size line');
    }
    if (!/^[0-9a-fA-F]+$/.test(sizeText)) {
      throw new TrailerParseError(`invalid chunk size: ${sizeText}`);
    }
    const chunkSize = parseInt(sizeText, 16);

    if (chunkSize === 0) {
      break;
    }

    if (pos + chunkSize > len) {
      throw new TrailerParseError('chunk data extends past end of stream');
    }
    bodyParts.push(bytes.subarray(pos, pos + chunkSize));
    bodyLength += chunkSize;
    pos += chunkSize;

    if (pos + 1 >= len || bytes[pos] !== CR || bytes[pos + 1] !== LF) {
      throw new TrailerParseError('chunk data is not followed by CRLF');
    }
    pos += 2;
  }

  /** @type {Map<string, string>} */
  const trailers = new Map();

  // After the final chunk we expect CRLF, then trailer lines, then CRLF.
  // A stream that ends immediately after the final chunk's CRLF is treated as
  // having an empty trailer section.
  for (;;) {
    if (pos >= len) {
      // No trailer section at all: the body ended at the final chunk.
      break;
    }
    let lineEnd = -1;
    for (let i = pos; i + 1 < len; i++) {
      if (bytes[i] === CR && bytes[i + 1] === LF) {
        lineEnd = i;
        break;
      }
    }
    if (lineEnd === -1) {
      throw new TrailerParseError('unterminated trailer line');
    }
    const line = parseTrailerLine(bytes, pos, lineEnd);
    pos = lineEnd + 2;
    if (line === null) {
      break;
    }
    trailers.set(line.name, line.value);
  }

  if (pos !== len) {
    throw new TrailerParseError('trailing bytes after trailer section');
  }

  const body = new Uint8Array(bodyLength);
  let offset = 0;
  for (const part of bodyParts) {
    body.set(part, offset);
    offset += part.length;
  }

  return { body, trailers };
}
