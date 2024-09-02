import { createWriteStream, existsSync, unlinkSync } from 'node:fs';
import { LineSearch } from '../src/LineSearch';

describe('should add and get entries', () => {
  it('small file', async () => {
    const lineSearch = new LineSearch(`${__dirname}/smallTestingFile.txt`);

    await lineSearch.init(true);

    expect(await lineSearch.search(0)).toEqual('one');
    expect(await lineSearch.search(1)).toEqual('two');
    expect(await lineSearch.search(2)).toEqual('three');

    let error: Error | null = null;
    try {
      await lineSearch.search(3);
    } catch (e) {
      error = e as Error;
    }

    expect(error?.message).toEqual('Line 3 not found');
  });

  describe('large file', () => {
    const mySecretValueToFind = 'earthIsNotFlat';
    const testingFilePath = `${__dirname}/largeTestingFile.txt`;
    const maxLines = 100000000;
    const mySecretLine = maxLines - 10;

    beforeAll(async () => {
      if (!existsSync(testingFilePath)) {
        const writeStream = createWriteStream(testingFilePath, {
          highWaterMark: 256 * 1024,
        });

        for (let i = 0; i < maxLines; i++) {
          if (i % 1000000 === 0) {
            console.log(`wrote ${i} lines`);
          }
          if (
            !writeStream.write(
              `${i === mySecretLine ? mySecretValueToFind : 'dummy line data'}\n`,
            )
          ) {
            await new Promise<void>((resolve) =>
              writeStream.once('drain', () => resolve()),
            );
          }
        }

        writeStream.end();
        await new Promise((resolve) => {
          writeStream.on('finish', resolve);
        });

        console.log('mySecretLine:', mySecretLine);
      }
    }, 12000000);

    afterAll(() => {
      unlinkSync(testingFilePath);
    });

    it('should find line with hash table shards', async () => {
      const lineSearch = new LineSearch(testingFilePath);

      console.time(`huge indexing took`);
      await lineSearch.init(true);
      console.timeEnd(`huge indexing took`);
      console.time(`huge file took`);
      expect(await lineSearch.search(mySecretLine)).toEqual(
        mySecretValueToFind,
      );
      console.timeEnd(`huge file took`);
    }, 1200000);
  });
});
