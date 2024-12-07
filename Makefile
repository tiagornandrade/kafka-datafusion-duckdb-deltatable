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

