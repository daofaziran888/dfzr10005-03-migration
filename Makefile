.PHONY: install backend frontend

install:
	cd backend && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
	cd frontend && npm install

backend:
	cd backend && ./run.sh

frontend:
	cd frontend && npm run dev

# ---- 生命周期台账：迁移、演练与核对 ----

PY := backend/.venv/bin/python
export PYTHONPATH := backend

preflight:
	./scripts/preflight.sh

migrate:
	$(PY) -m app.persistence_cli migrate --with-dev-seed

verify:
	$(PY) -m app.persistence_cli verify

snapshot:
	$(PY) -m app.persistence_cli snapshot --label pre-cutover

test:
	$(PY) -m unittest discover -s backend/tests -v
