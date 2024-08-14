import { createReadStream, createWriteStream, existsSync } from 'node:fs';
import { Transform } from 'node:stream';

const FILE_VALUE_SIZE_BYTES = 8;

export class LineDB {
  constructor(private readonly filename = 'index.idx') {}

  hasDbIndexFile() {
    return existsSync(this.filename);
  }

  async search(lineNumber: number): Promise<number | null> {
    return new Promise((resolve, reject) => {
      const readStreamConfig = {
        start: lineNumber * FILE_VALUE_SIZE_BYTES,
        end: lineNumber * FILE_VALUE_SIZE_BYTES + FILE_VALUE_SIZE_BYTES - 1,
        highWaterMark: FILE_VALUE_SIZE_BYTES,
      };
      const readStream = createReadStream(this.filename, readStreamConfig);

      let foundData: number | null = null;

      readStream.on('data', (chunk: Buffer) => {
        foundData = Number(chunk.readBigUInt64BE());
      });

      readStream.on('end', () => {
        resolve(foundData);
      });

      readStream.on('error', (error) => {
        reject(error);
      });
    });
  }

  pipe() {
    const writeStream = createWriteStream(this.filename);

    const transformToIndex = new Transform({
      transform(chunk, encoding, done) {
        const indexBuffer = Buffer.alloc(FILE_VALUE_SIZE_BYTES);
        indexBuffer.writeBigUInt64BE(BigInt(chunk.toString()));
        done(null, indexBuffer);
      },
    });

    transformToIndex.pipe(writeStream);

    return transformToIndex;
  }
}
