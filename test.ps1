mkdir test_output
cd test_output
vixutil -i  -m continuous.pkl -t term.pkl -c cash.pkl --calendar calendar.pkl -w continuous_weights.pkl

vixutil -i  -m continuous.csv -t term.csv -c cash.csv --calendar calendar.csv -w continuous_weights.csv


vixutil -i  -m continuous.html -t term.html -c cash.html --calendar calendar.html -w continuous_weights.html

vixutil -i  -m continuous.xlsx -t term.xlsx  -c cash.xlsx --calendar calendar.xlsx -w continuous_weights.xlsx
cd ..