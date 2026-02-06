# Quick Start Guide - 5 Minutes to Running

**For impatient developers who just want to get started!**

---
#Usage
Best to use on Ubuntu server, either locally server or work server

*run python load_env.py

## 🚀 30-Second Quick Start (Local)

```bash
# 1. Setup
python -m venv venv
source venv/bin/activate              # or venv\Scripts\activate on Windows
pip install -r requirements.txt
pip install rq redis

# 2. Database
python -c "from phase5.database import init_db; init_db()"

# 3. Start (in separate terminals)

# Terminal 1 - Backend
activate venv: source venv/bin/activate 
python3 run_phase5.py

# Terminal 2 - Frontend
cd frontend && npm install && npm run dev

# Terminal 3 - Redis 
sudo systemctl start redis-server
redis-server #(if not already running)

# Terminal 4 - Worker (for background jobs)
rq worker phase5
```

**Done!** Open http://localhost:3000

---

## 🐧 5-Minute Ubuntu Deployment

```bash
# 1. Install prerequisites
sudo apt-get update && sudo apt-get install -y python3-pip python3-venv \
  nodejs npm redis-server postgresql postgresql-contrib git supervisor nginx

# 2. Deploy
sudo git clone <repo> /var/www/docgen
cd /var/www/docgen
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt gunicorn psycopg2-binary rq redis

# 3. Database
sudo -u postgres psql -c "CREATE USER docgen WITH PASSWORD 'pass'; CREATE DATABASE docgen OWNER docgen;"
DATABASE_URL=postgresql://docgen@localhost/docgen python -c "from phase5.database import init_db; init_db()"

# 4. Configuration
echo "DATABASE_URL=postgresql://docgen@localhost/docgen" >> .env
echo "REDIS_URL=redis://localhost:6379/0" >> .env
echo "DEBUG=False" >> .env

# 5. Start with supervisor
sudo nano /etc/supervisor/conf.d/docgen.conf
# Copy configuration from main README
sudo supervisorctl reread && sudo supervisorctl update

# 6. Frontend
cd frontend && npm install && npm run build

# 7. Nginx
sudo nano /etc/nginx/sites-available/docgen
# Copy nginx config from main README
sudo ln -s /etc/nginx/sites-available/docgen /etc/nginx/sites-enabled/
sudo systemctl restart nginx
```

**Done!** Access via https://yourdomain.com

---

## 🎯 Using the Web Interface

### Crawl Websites
1. Click "Crawl" → Enter domains/keywords → Click "Start" → Monitor progress

### Send Emails
1. Click "Email" → Upload recipients → Create template → Click "Send"

### Submit Forms
1. Click "Forms" → Add URLs → Enter form data → Click "Submit"

### Generate Documents
1. Click "Generate" → Select document type → Fill info → Click "Generate"

### View Results
1. Click "History" → Filter by type/date → Download results

---

## 🔧 Common Tasks

### Check Status
```bash
curl http://localhost:8000/api/documents/health
curl http://localhost:8000/docs  # API documentation
```

### View Logs
```bash
tail -f logs/phase5.log
```

### Stop Services
```bash
# Local: Ctrl+C in each terminal
# Ubuntu: sudo supervisorctl stop docgen-api docgen-worker
```

### Restart Services
```bash
# Local: Stop and run again
# Ubuntu: sudo supervisorctl restart docgen-api docgen-worker
```

### Check Database
```bash
sqlite3 phase5.db "SELECT COUNT(*) FROM crawl_job;"
# OR PostgreSQL:
psql -U docgen -d docgen -c "SELECT COUNT(*) FROM crawl_job;"
```

---

## ⚠️ Common Issues & Fixes

| Issue | Fix |
|-------|-----|
| Port 8000 in use | `lsof -i :8000` then `kill -9 <PID>` |
| Redis not found | `redis-server` in new terminal |
| Database error | Check DATABASE_URL, ensure it exists |
| Frontend not loading | `cd frontend && npm install` then `npm run dev` |
| Jobs not processing | `rq worker phase5` must be running |

---

## 📚 Next Steps

- Read main [README.md](README.md) for full documentation
- See [PHASE3_README.md](PHASE3_README.md) for Form Submission details
- Check API docs at http://localhost:8000/docs
- Review deployment section in main README for production

---

**That's it! You're ready to use the application! 🎉**

For detailed setup options, see [README.md](README.md)
