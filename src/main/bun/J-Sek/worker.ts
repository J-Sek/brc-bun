import * as fs from 'node:fs';

declare var self: Worker;

const cities = new Map<string, CityValues>();
type CityValues = { min: number; max: number; avg: number; count: number };

self.onmessage = async (event: MessageEvent) => {
  const { fileName, start, end } = event.data;
  await readStream(
    fs.createReadStream(fileName, { start, end }),
    acceptLine,
    () => {
      postMessage(cities);
      process.exit();
    }
  );
};

function acceptLine(line: string) {
  if (line.length < 3) return; // skip empty lines

  const [city, value] = line.split(";");
  const v = Number(value);

  if (!cities.has(city)) {
    cities.set(city, { min: v, max: v, avg: v, count: 1 });
  } else {
    const values = cities.get(city)!;

    if (values.min > v) values.min = v;
    if (values.max < v) values.max = v;

    // hack: average is not expected to change after certain number of measurements
    if (values.count < Number.MAX_VALUE / 100) {
      values.avg = (values.avg * values.count + v) / (values.count + 1);
    }
    values.count += 1;
  }
}

async function readStream(
  stream: fs.ReadStream,
  accept: (line: string) => void,
  done: () => void
) {
  const lineBreak = "\n".charCodeAt(0);
  const textDecoder = new TextDecoder();

  // max line length is 128
  let lineBuffer = new Uint8Array(128);
  let lineBufferLen = 0;

  for await (const chunk of stream) {
    for (let i = 0; i < chunk.length; i++) {
      if (chunk[i] === lineBreak) {
        const line = textDecoder.decode(lineBuffer.slice(0, lineBufferLen));
        lineBufferLen = 0;
        accept(line);
      } else {
        lineBuffer[lineBufferLen] = chunk[i];
        lineBufferLen++;
      }
    }
  }

  if (lineBufferLen > 0) {
    accept(textDecoder.decode(lineBuffer.slice(0, lineBufferLen)));
  }

  done();
}
