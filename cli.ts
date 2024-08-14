#!/usr/bin/env node
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import { LineSearch } from './src/LineSearch';

const argv = yargs(hideBin(process.argv))
  .command(
    '$0 <filename> <line>',
    'Get content of defined line in file',
    (yargs) => {
      yargs
        .positional('filename', {
          describe: 'Name of the file to process',
          type: 'string',
          demandOption: true,
        })
        .positional('line', {
          describe: 'Line number',
          type: 'number',
          demandOption: true,
        })
        .option('force', {
          alias: 'f',
          describe: 'Force DB index to reindex file again',
          type: 'boolean',
          default: false,
          demandOption: false,
        });
    },
    (argv) => {},
  )
  .help().argv as unknown as {
  line: number;
  filename: string;
  force: boolean;
} & Record<string, unknown>;

async function run() {
  const lineSearch = new LineSearch(argv.filename);

  await lineSearch.init(argv.force);

  const result = await lineSearch.search(argv.line);
  console.log('Result:', result);
}

run();
