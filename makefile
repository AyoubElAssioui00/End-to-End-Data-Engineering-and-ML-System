# =========================
# Project configuration
# =========================
PROJECT_NAME=End-to-End-Data-Engineering-and-ML-System
VENV=.venv
DOCKER_COMPOSE=docker compose
KAFKA_CONTAINER=kafka
BOOTSTRAP_SERVER=kafka:9092

# =========================
# Virtual environment
# =========================
.PHONY: venv
venv:
	uv venv
	@echo "Virtual environment created"

.PHONY: activate
activate:
	@echo "Run the following command:"
	@echo "source $(VENV)/bin/activate"

.PHONY: deactivate
deactivate:
	@echo "To deactivate the venv, run:"
	@echo "deactivate"

# =========================
# Dependencies
# =========================
.PHONY: install
install:
	uv sync

# =========================
# Docker Compose
# =========================
.PHONY: up
up:
	$(DOCKER_COMPOSE) up -d
	@echo "Docker containers started"

.PHONY: down
down:
	$(DOCKER_COMPOSE) down
	@echo "Docker containers stopped"

.PHONY: restart
restart: down up

# =========================
# Kafka topics
# =========================
.PHONY: create-topics
create-topics:
	docker exec -it $(KAFKA_CONTAINER) kafka-topics \
		--bootstrap-server $(BOOTSTRAP_SERVER) \
		--create --if-not-exists \
		--topic network_flows \
		--partitions 1 --replication-factor 1
	docker exec -it $(KAFKA_CONTAINER) kafka-topics \
		--bootstrap-server $(BOOTSTRAP_SERVER) \
		--create --if-not-exists \
		--topic normal_traffic \
		--partitions 1 --replication-factor 1
	docker exec -it $(KAFKA_CONTAINER) kafka-topics \
		--bootstrap-server $(BOOTSTRAP_SERVER) \
		--create --if-not-exists \
		--topic anomaly_alerts \
		--partitions 1 --replication-factor 1
	@echo "Kafka topics created"

.PHONY: list-topics
list-topics:
	docker exec -it $(KAFKA_CONTAINER) kafka-topics \
		--bootstrap-server $(BOOTSTRAP_SERVER) --list

# =========================
# Application commands
# =========================
.PHONY: simulate
simulate:
	uv run python main.py simulate

.PHONY: detect
detect:
	uv run python main.py detect

.PHONY: dashboard
dashboard:
	uv run python scripts/streamlit_dashboard.py

# =========================
# Full setup
# =========================
.PHONY: setup
setup: install up create-topics
	@echo "Project fully set up"
