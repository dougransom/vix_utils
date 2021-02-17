mkdir test_output
cd test_output
vixutil -i  -m continuous.pkl -t term.pkl -c cash.pkl --calendar calendar.pkl

vixutil -i  -m continuous.csv -t term.csv -c cash.csv --calendar calendar.csv

vixutil -i  -m continuous.parquet -t term.parquet -c cash.parquet --calendar calendar.parquet

cd ..