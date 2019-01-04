export PYTHONPATH=.:vendor
cd ~/SpotManager-ETL
python spot/spot_manager.py --settings=./examples/config/etl_settings.json 2>~/SpotManager/etl.error.log

