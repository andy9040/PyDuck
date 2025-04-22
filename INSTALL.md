# Installing PyDuck 🦆

This document provides step-by-step instructions to install and set up PyDuck for both users and developers.  
PyDuck is a high-performance Python library for machine learning data preprocessing built on [DuckDB](https://duckdb.org) and designed with a [Pandas](https://pandas.pydata.org/)‑like API.

---

## 🛠️ Prerequisites

Ensure you have **Python 3.7 or above** installed.

Check your version:
```bash
python3 --version
```

If Python is not installed, download it from:  https://www.python.org/downloads/

Also verify that pip is available:
```
python3 -m ensurepip --upgrade
```

## 📦 Step 1: Install PyDuck (for Users)
To install the latest PyPI version:
```
pip install pyduck
```

## 👨‍💻 Step 2: Clone the Repository (for Developers)
```
git clone https://github.com/your-username/pyduck.git
cd pyduck
```

## 📋 Step 3: Install Dependencies
```
pip install -r requirements.txt
```

Dependencies include:
- duckdb
- pandas
- numpy
- pytest (for testing)

## 🧪 Step 4: Install Locally in Editable Mode (for Development)
From the outer `pyduck/` directory:
```
pip install -e .
```

## ✅ Step 5: Run the Test Suite
To verify everything is working,
```
python3 -m pytest testing/
```

## Step 6: How to Run Evaluation Sequence
Go to eval folder,
```
cd eval
```
And run benchmarking sequences that are defined in each `Tester.py` file through:
```
python3 test_all.py
```