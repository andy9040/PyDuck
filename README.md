# PyDuck
PyDuck 🦆 is a high-performance Python library for ML data preprocessing, built on DuckDB with a Pandas-like API. It enables fast, multi-threaded, out-of-core processing, handling large datasets efficiently. PyDuck accelerates ML workflows by optimizing queries while ensuring seamless integration with Pandas and other data tools. 🚀

## To Use
Simply import like any other package
```
pip install pyduck
```

## Set Up for Developers

Install all requirements.
```
pip install -r requirements.txt
```

## Testing for Developers
Make sure you are in the outer PyDuck directory.

To install the package locally for testing (recommended):
```
pip install -e .
```

To run the testing suite in `testing/`
```
python3 -m pytest testing/ 
```


##### CAEN Set up [Work in Progress]

python3
```
curl -o python-installer.exe https://www.python.org/ftp/python/3.13.2/python-3.13.2-amd64.exe
mkdir python310
unzip python-3.10.11-embed-amd64.zip -d python310
find $HOME -name python.exe
alias python="/home/andrenan/PyDuck/python310/python.exe"
echo 'alias python="/home/andrenan/PyDuck/python310/python.exe"' >> ~/.bashrc
source ~/.bashrc
```

pip3
```
curl -o get-pip.py https://bootstrap.pypa.io/pip/3.6/get-pip.py
python3 get-pip.py

```