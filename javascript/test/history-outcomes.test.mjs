import assert from 'node:assert/strict';
import test from 'node:test';
import * as wire from '../js/abstraction/logging/index.mjs';

const encode = new TextEncoder();
const decode = new TextDecoder();
function transport(value) {
  return {async exchangeFrame(raw) {
    const request = JSON.parse(decode.decode(raw));
    return encode.encode(JSON.stringify({version: 1, service: request.service,
      method: request.method, ok: true, payload: {value}}));
  }};
}

test('new clients return typed policy, bounds and protected waiting outcomes', async () => {
  for (const outcome of ['not_permitted', 'forbidden', 'invalid', 'unavailable', 'os_authorization_required', 'os_authorization_pending']) {
    const reply = transport({outcome, records: [], next: 'supplied', at_end: false});
    const source = await new wire.SourceReaderClient(reply).read('windows/application', 'supplied', 1n, 65536n);
    assert.deepEqual(source, {outcome, records: [], next: 'supplied', atEnd: false});
    const target = wire.newApplicationLogTarget();
    target.selection = 'all_apps';
    assert.equal((await new wire.ApplicationHistoryReaderClient(reply).read(target, 'supplied', 1n, 65536n)).outcome, outcome);
    assert.equal((await new wire.ApplicationHistoryObserverClient(reply).observe(target, 'supplied', 1n, 65536n, 0n)).outcome, outcome);
  }
  await assert.rejects(new wire.SourceReaderClient(transport({outcome: 'future_action', records: [], next: '', at_end: false})).read('windows/application', '', 1n, 65536n));
});

test('catalogue carries future descriptive constraints while usability stays closed', async () => {
  const catalog = await new wire.SourceReaderClient(transport({outcome: 'catalog', sources: [
    {id: 'windows/application', availability: 'available', limitations: ['future_constraint']},
  ]})).sources();
  assert.equal(catalog.outcome, 'catalog');
  assert.deepEqual(catalog.sources[0].limitations, ['future_constraint']);
  assert.match(decode.decode(wire.encodeSourceCatalog(catalog)), /future_constraint/);
  for (const invalid of [{sources: []}, {sources: [], outcome: 'future_action'},
    {outcome: 'catalog', sources: [{id: 'windows/application', availability: 'future_state', limitations: []}]}]) {
    assert.throws(() => wire.decodeSourceCatalog(encode.encode(JSON.stringify(invalid))));
  }
});

test('published legacy Page spelling remains unchanged', () => {
  const raw = wire.encodePage({outcome: 'invalid_request', records: [], next: 'supplied', atEnd: false});
  assert.equal(decode.decode(raw), '{\n  "outcome": "invalid_request",\n  "records": [],\n  "next": "supplied",\n  "at_end": false\n}\n');
  assert.throws(() => wire.decodePage(encode.encode('{"outcome":"invalid","records":[],"next":"supplied","at_end":false}')));
});
