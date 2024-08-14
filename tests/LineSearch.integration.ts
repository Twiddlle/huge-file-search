import { createWriteStream, existsSync, unlinkSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { LineSearch } from '../src/LineSearch';

describe('should add and get entries', () => {
  it('small file', async () => {
    const lineSearch = new LineSearch(`${__dirname}/smallTestingFile.txt`);

    await lineSearch.init(true);

    expect(await lineSearch.search(0)).toEqual('one');
    expect(await lineSearch.search(1)).toEqual('two');
    expect(await lineSearch.search(2)).toEqual('three');
    expect(await lineSearch.search(3)).toBeNull();
  });

  describe('large file', () => {
    const mySecretValueToFind = 'earthIsNotFlat';
    const testingFilePath = `${__dirname}/largeTestingFile.txt`;
    const maxLines = 1000000;
    const mySecretLine = maxLines - 10;

    beforeAll(async () => {
      if (!existsSync(testingFilePath)) {
        const writeStream = createWriteStream(testingFilePath);

        for (let i = 0; i < maxLines; i++) {
          if (i === mySecretLine) {
            writeStream.write(`${mySecretValueToFind}\n`);
            continue;
          }
          writeStream.write(`${randomUUID()}\n`);
        }

        writeStream.end();
        await new Promise((resolve) => {
          writeStream.on('finish', resolve);
        });

        console.log('mySecretLine:', mySecretLine);
      }
    }, 120000);

    afterAll(() => {
      unlinkSync(testingFilePath);
    });

    it('should find line with hash table shards', async () => {
      const lineSearch = new LineSearch(testingFilePath);

      await lineSearch.init(true);
      console.time(`huge file took`);
      expect(await lineSearch.search(mySecretLine)).toEqual(
        mySecretValueToFind,
      );
      console.timeEnd(`huge file took`);
    }, 120000);
  });
});
