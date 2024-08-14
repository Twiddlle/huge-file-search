import { LineDB } from './LineDB';
import { createReadStream, existsSync } from 'node:fs';
import { basename } from 'node:path';
import * as readline from 'node:readline';

export class LineSearch {
  private readonly lineDB: LineDB;

  constructor(private readonly filePath: string) {
    this.lineDB = new LineDB(`indices/${basename(filePath)}.idx`);
  }

  init(force = false) {
    if (!existsSync(this.filePath)) {
      throw new Error(`File ${this.filePath} does not exist`);
    }

    if (this.lineDB.hasDbIndexFile() && !force) {
      return;
    }

    console.log('Indexing file...');

    const readStream = readline.createInterface({
      input: createReadStream(this.filePath),
    });

    const lineDbStream = this.lineDB.pipe();

    return new Promise<void>((resolve, reject) => {
      let lineCount = 1;
      let filePosition = 0;
      readStream.on('line', (line) => {
        if (line !== '') {
          lineDbStream.write(Buffer.from(`${filePosition}`));
          filePosition += line.length + 1; // +1 for new line character
          lineCount++;
        }
      });
      readStream.on('error', (error) => reject(error));
      readStream.on('close', () => lineDbStream.end());

      lineDbStream.on('end', () => {
        console.log('Indexing finished');
        resolve();
      });
    });
  }

  async search(line: number): Promise<string | null> {
    const positionOfLine = await this.lineDB.search(line);
    if (positionOfLine === null) {
      return null;
    }

    return new Promise((resolve, reject) => {
      // we are about to find original byte position of the file line
      const readStream = readline.createInterface({
        input: createReadStream(this.filePath, {
          start: positionOfLine,
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
