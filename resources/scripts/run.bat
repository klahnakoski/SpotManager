SET PYTHONPATH=.
python spot\spot_manager.py --settings=./resources/config/etl_settings.json
python spot\spot_manager.py --settings=./resources/config/es_spot_settings.json
