# Data Processing With Actors, without REST endpoint

To run the app, call:

```bash
cargo run -- --from 2020-07-03T12:00:09Z  --symbols LYFT,MSFT,AAPL,UBER,LYFT,AMD,GOOG --out-file outf.csv
```

To load params from file, call:

```bash
cargo run -- --from 2020-07-03T12:00:09Z  --symbol-file sp500.dec.2022.txt --out-file outf.csv
```

processed performance indicators will be written into the file, they are not printed to the console.