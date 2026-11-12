# Data Engineering Assessment

A small PySpark pipeline. The steps are defined in `config/pipeline.yaml` and run over the trade data in `data/input/instruments.csv`, producing daily metrics (total notional, average price, total volume and VWAP) per instrument, date and currency.

## How to run

```
pip install -r requirements.txt
python -m src.main
pytest
```

Run the commands in terminal, make sure there are python and Java installed.

## Layout

- `src/core/config.py`: loads the YAML into a `PipelineConfig` (source, transformations, output columns, validation rules).
- `src/core/engine.py`: `PipelineEngine` reads the CSV, cleans it, runs each transformation in order, applies the validation rules and selects the output columns.
- `src/transforms/base.py`: the filter, expression and aggregate steps, plus `create_transform`, which builds the right one from a config entry.
- `src/transforms/functions.py`: the functions expression steps can call (`multiply`, `divide`, `current_timestamp`). `divide` returns null on a zero denominator instead of failing.

## Data issues and solution

Going through `instruments.csv` There are some issues:

1. **Whitespace in tickers.** Line 22 has `" AAPL"` with a space. So all string columns are trimmed to prevent miscounting.
2. **Mixed date formats.** NFLX on line 25 uses `01/15/2024`. `trade_date` is in  `MM-dd-yyyy`, I make it to timestamp and turn to `yyyy-MM-dd`.
3. **Zero and negative volumes.** AMZN has `volume = 0` on lines 18–19 and MSFT has `-500` on line 27, so they're dropped during cleaning. The `not_null`/`positive` also checks after aggregation.
4. **Negative prices** (`0.00`, `-5.00`, empty). `price` is cast to double and the existing `price > 0` filter in the config removes them.
5. **Duplicate rows.** META on lines 20–21 has two identical rows. There's no trade id or timestamp, so I can't tell a duplicate from two fills of the same size at the same price. I kept both rather than risk understating volume.
6. **Mixed currencies.** GOOGL on 2024-01-15 has both USD and EUR rows (lines 5–6 and 23–24). Without FX rates these can't be added together, so the main output is USD only and the non-USD rows are written to `data/output/non_usd` (also kept on `engine.non_usd_df`) so nothing is silently lost. 
