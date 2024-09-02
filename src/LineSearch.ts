import { LineDB } from './LineDB';
import {
  createReadStream,
  existsSync,
  readFileSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { basename } from 'node:path';
import * as readline from 'node:readline';
import { Worker } from 'worker_threads';
import { LineDBWorkerData } from './lineDBWorker';

export class LineSearch {
  private readonly metaFile: string;
  private metaData?: {
    shards: number;
    indices: { lines: number; filePosition: number }[];
  };
  private fileSize: number;
  private initialized = false;

  constructor(
    private readonly filePath: string,
    private readonly shards = 4,
  ) {
    this.metaFile = `indices/${basename(this.filePath)}.meta.idx`;
    this.fileSize = statSync(this.filePath).size;
  }

  async init(force = false) {
    if (!existsSync(this.filePath)) {
      throw new Error(`File ${this.filePath} does not exist`);
    }

    if (this.hasMeta() && !force) {
      this.metaData = this.getMeta();

      if (this.metaData.shards !== this.shards) {
        throw new Error(
          `Shards count ${this.shards} is different than stored in meta file ${this.metaData.shards}`,
        );
      }
      this.initialized = true;
      return;
    }

    console.log('Indexing file...');

    const lengthPerFile = Math.ceil(this.fileSize / this.shards);

    const indexResults = await Promise.all(
      new Array(this.shards).fill(0).map((_, index) => {
        return this.indexData(
          this.getIndexFileNameByShard(index),
          index * lengthPerFile,
          lengthPerFile,
        );
      }),
    );

    this.storeMeta({
      shards: this.shards,
      indices: (indexResults as { lines: number; filePosition: number }[]).map(
        (result) => {
          return {
            lines: result.lines,
            filePosition: result.filePosition,
          };
        },
      ),
    });

    this.initialized = true;

    console.log('indexResults', indexResults);
  }

  private storeMeta(meta: typeof LineSearch.prototype.metaData) {
    writeFileSync(this.metaFile, JSON.stringify(meta));
  }

  private hasMeta(): boolean {
    return existsSync(this.metaFile);
  }

  private getMeta() {
    if (!this.metaData) {
      if (!this.hasMeta()) {
        throw new Error(`Meta file ${this.metaFile} does not exist`);
      }
      this.metaData = JSON.parse(readFileSync(this.metaFile, 'utf-8'));
    }
    return this.metaData!;
  }

  private getIndexFileNameByShard(shard: number) {
    return `indices/${basename(this.filePath)}.${shard}.idx`;
  }

  private indexData(
    indexFile: string,
    offset: number,
    lengthToIndex: number,
  ): Promise<{ lines: number }> {
    const worker = new Worker('./src/lineDBWorker.js', {
      workerData: {
        sourceFileName: this.filePath,
        indexFileName: indexFile,
        offset: offset,
        lengthToIndex,
      } satisfies LineDBWorkerData,
    });

    return new Promise((resolve, reject) => {
      worker.on('message', (message) => {
        try {
          const messageData = JSON.parse(message);
          if (messageData.result === 'end') {
            resolve(messageData.data);
          } else {
            reject(new Error(messageData.data));
          }
        } catch (e) {
          reject(e);
        }
      });
    });
  }

  private getIndexLineAndShard(line: number) {
    let lineCount = 0;
    let lastLineCount = 0;
    let filePosition = 0;
    const indices = this.getMeta().indices;
    for (const shard in indices) {
      lineCount += indices[shard].lines;
      if (line < lineCount) {
        return {
          shard: parseInt(shard),
          indexLine: line - lastLineCount,
          filePosition,
        };
      }
      lastLineCount += indices[shard].lines;
      filePosition += indices[shard].filePosition;
    }

    return undefined;
  }

  async search(line: number): Promise<string | null> {
    if (!this.initialized) {
      throw new Error('LineSearch not initialized');
    }

    const shardAndLine = this.getIndexLineAndShard(line);

    if (!shardAndLine) {
      throw new Error(`Line ${line} not found`);
    }

    const lineDb = new LineDB(this.getIndexFileNameByShard(shardAndLine.shard));
    const positionOfLine = await lineDb.search(shardAndLine.indexLine);
    if (positionOfLine === null) {
      return null;
    }
    const positionInFile = shardAndLine.filePosition + positionOfLine;

    return new Promise((resolve, reject) => {
      // we are about to find original byte position of the file line
      const readStream = readline.createInterface({
        input: createReadStream(this.filePath, {
          start: positionInFile,
        }),
      });

      let foundData: string | null = null;

      readStream.on('line', (chunk: Buffer) => {
        if (!foundData) {
          foundData = chunk.toString();
        }
      });

      readStream.on('close', () => {
        resolve(foundData);
      });

      readStream.on('error', (error) => {
        reject(error);
      });
    });
  }
}
