from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:1713@localhost:5432/sche')
# schtasks /Create /SC HOURLY /TN B:\Python\venv\Scripts\activate.bat /TR "B:\Python\Pipeline\main.py"