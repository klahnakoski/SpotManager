SET PYTHONPATH=.
python spot\spot_manager.py --settings=./examples/config/etl_settings.json
python spot\spot_manager.py --settings=./examples/config/es_settings.json
