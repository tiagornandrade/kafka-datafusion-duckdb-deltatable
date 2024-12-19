up:
	docker-compose up -d

down:
	docker-compose down

fakedata:
	python3 events/src/gen.py

ingestion:
	python3 ingestion/src/main.py

run: fakedata
	sleep 1
	make -B ingestion

landing-to-raw:
	python3 promotion/src/landing_to_raw.py

raw-to-trusted:
	python3 promotion/src/raw_to_trusted.py

trusted-to-refined:
	python3 promotion/src/trusted_to_refined.py