import * as os from "node:os";
import { createReadStream } from "node:fs";

const fileName = Bun.argv[2];

const fileSize = Bun.file(fileName).size;

const workerCount = os.cpus().length - 1;

let lastOffset = 0;
const tasks = [];
while (lastOffset < fileSize) {
  const start = lastOffset;
  let end = Math.min(fileSize, start + 100 + fileSize / workerCount);
  if (end < fileSize) {
    end = await moveToClosestLineBreak(end);
  }
  tasks.push(
    new Promise<Map<string, CityValues>>((resolve, reject) => {
      const worker = new Worker(new URL("worker.ts", import.meta.url).href);
      worker.postMessage({ fileName, start, end });
      worker.onmessage = (e) => resolve(e.data);
      worker.onerror = (e) => { console.error(e); reject(); };
    })
  );
  lastOffset = end;
}

const groups = await Promise.all(tasks);
const cities = aggregate(groups);
printResults();

type CityValues = { min: number; max: number; avg: number; count: number };

function aggregate(groups: Map<string, CityValues>[]) {
  const [firstGroup, ...rest] = groups;
  const allCities = firstGroup;

  rest.forEach(g => {
    [...g.entries()].forEach(([city, gv]) => {
      if (!allCities.has(city)) {
        allCities.set(city, gv);
      } else {
        const values = allCities.get(city)!;
        if (values.min > gv.min) values.min = gv.min;
        if (values.max < gv.max) values.max = gv.max;
        const newCount = values.count + gv.count;
        if ((values.count + gv.count) < (Number.MAX_VALUE / 100)) {
          // hack in condition above to avoid int overflow
          values.avg = (values.avg * values.count + gv.avg * gv.count) / newCount;
          values.count = newCount;
        }
      }
    })
  })

  return allCities;
}

function printResults() {
  const result =
    "{" +
    [...cities.keys()]
      .toSorted()
      .map((city) => {
        const values = cities.get(city)!;
        const out = [
          values.min.toFixed(1),
          values.avg.toFixed(1),
          values.max.toFixed(1),
        ].join("/");
        return [city, out].join("=");
      })
      .join(", ") +
    "}";

  console.log(result);
}

function moveToClosestLineBreak(start: number): Promise<number> {
  return new Promise((resolve) => {
    const stream = createReadStream(fileName, { start, end: start + 128 });
    stream.on('readable', () => {
      const chunk = stream.read(128);
      const offset = chunk.indexOf("\n".charCodeAt(0));
      resolve(start + offset);
      stream.close();
    });
  });
}
