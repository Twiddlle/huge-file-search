import { workerData, parentPort } from 'node:worker_threads';
import * as readline from 'node:readline';
import { createReadStream } from 'node:fs';
import { LineDB } from './LineDB';

export interface LineDBWorkerData {
  sourceFileName: string;
  indexFileName: string;
  offset: number;
  lengthToIndex: number;
}

if (!parentPort) {
  throw new Error('No parent port');
}

function indexData(data: LineDBWorkerData) {
  const readStream = createReadStream(data.sourceFileName, {
    start: data.offset,
    highWaterMark: 256 * 1024,
    // autoClose: false,
  });
  const lineStream = readline.createInterface({
    input: readStream,
  });

  const lineDB = new LineDB(data.indexFileName);

  let lineCount = 0;
  let filePosition = 0;

  const writeLineDbStream = lineDB.createWriteStreamToIndex();

  lineStream.on('error', (error) =>
    parentPort!.postMessage(
      JSON.stringify({ result: 'error', data: error.message }),
    ),
  );
  lineStream.on('close', () => {
    writeLineDbStream.end();
  });

  writeLineDbStream.on('end', () => {
    console.log('Indexing finished', data.indexFileName);
    parentPort!.postMessage(
      JSON.stringify({
        result: 'end',
        data: {
          lines: lineCount,
          filePosition,
          indexFileName: data.indexFileName,
        },
      }),
    );
  });

  lineStream.on('line', (line) => {
    if (filePosition === 0 && line === '') {
      return;
    }

    lineCount++;
    if (writeLineDbStream.writable) {
      writeLineDbStream.write(Buffer.from(`${filePosition}`));
    }

    if (lineCount % 1000000 === 0) {
      console.log(`Indexed ${lineCount} lines`, data.indexFileName);
    }

    //todo: handle \r\n
    filePosition += line.length + 1; // +1 for new line character

    if (filePosition >= data.lengthToIndex) {
      lineStream.close();
      lineStream.removeAllListeners();
      readStream.destroy();
      return;
    }
  });
}

indexData(workerData as LineDBWorkerData);
