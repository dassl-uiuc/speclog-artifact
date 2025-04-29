# Script to generate plots for all benchmark results

# Usage: ./plot_all.sh <output_directory> 

source ../common.sh

pushd rq1
python3 rq1.py 
mv *.pdf ../$1
popd 

pushd rq2
python3 rq2.py
mv *.pdf ../$1
popd

pushd rq3
python3 rq3.py
mv *.pdf ../$1
popd

pushd rq4
python3 rq4.py
mv *.pdf ../$1
popd

pushd rq5
python3 rq5.py
mv *.pdf ../$1
popd

pushd rq6
python3 rq6.py
mv *.pdf ../$1
popd

pushd rq7
python3 rq7.py
mv *.pdf ../$1
popd

pushd rq8
python3 rq8.py
mv *.pdf ../$1
popd

pushd rq9
python3 rq9.py
mv *.pdf ../$1
popd

pushd rq10
python3 rq10.py
mv *.pdf ../$1
popd

echo "All plots have been generated and moved to $1"
